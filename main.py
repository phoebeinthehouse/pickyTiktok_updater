import os, json, time, random, re, asyncio, threading
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict

import gspread
from google.oauth2.service_account import Credentials

# ============= Config =============

@dataclass
class Config:
    spreadsheet_id: str
    worksheet_name: str

    log_every_n: int
    heartbeat_every_sec: int

    sleep1_min: float
    sleep1_max: float
    sleep2_min: float
    sleep2_max: float

    max_rounds: int
    stall_rounds: int

    headless: bool
    nav_timeout_ms: int
    extra_wait_sec: float

    write_batch_size: int
    write_pause_sec: float
    read_pause_sec: float


def env_str(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or str(v).strip() == "":
        raise ValueError(f"Missing required env: {name}")
    return v

def env_int(name: str, default: str) -> int:
    return int(os.getenv(name, default))

def env_float(name: str, default: str) -> float:
    return float(os.getenv(name, default))

def env_bool(name: str, default: str) -> bool:
    v = os.getenv(name, default).strip().lower()
    return v in ("1", "true", "yes", "y", "on")

def load_config() -> Config:
    return Config(
        spreadsheet_id=env_str("SPREADSHEET_ID"),
        worksheet_name=env_str("WORKSHEET_NAME", "Sheet1"),

        log_every_n=env_int("LOG_EVERY_N", "50"),
        heartbeat_every_sec=env_int("HEARTBEAT_EVERY_SEC", "120"),

        sleep1_min=env_float("SLEEP1_MIN", "1.5"),
        sleep1_max=env_float("SLEEP1_MAX", "3.0"),
        sleep2_min=env_float("SLEEP2_MIN", "6.0"),
        sleep2_max=env_float("SLEEP2_MAX", "10.0"),

        max_rounds=env_int("MAX_ROUNDS", "6"),
        stall_rounds=env_int("STALL_ROUNDS", "2"),

        headless=env_bool("HEADLESS", "true"),
        nav_timeout_ms=env_int("NAV_TIMEOUT_MS", "45000"),
        extra_wait_sec=env_float("EXTRA_WAIT_SEC", "2.5"),

        write_batch_size=env_int("WRITE_BATCH_SIZE", "80"),
        write_pause_sec=env_float("WRITE_PAUSE_SEC", "8.0"),
        read_pause_sec=env_float("READ_PAUSE_SEC", "1.0"),
    )

# ============= Logging / Utils =============

def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def log(msg: str) -> None:
    print(f"[{now_ts()}] {msg}", flush=True)

def human_sec(sec: float) -> str:
    sec = max(0.0, float(sec))
    if sec < 60:
        return f"{sec:.0f}s"
    m = sec / 60
    if m < 60:
        return f"{m:.1f}m"
    h = m / 60
    return f"{h:.2f}h"

def is_empty(x) -> bool:
    return x is None or str(x).strip() == ""

async def sleep_rand(a: float, b: float) -> None:
    await asyncio.sleep(random.uniform(float(a), float(b)))

class Heartbeat:
    def __init__(self, every_sec: int):
        self.every_sec = max(10, int(every_sec))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.is_set():
            log("heartbeat: still running")
            self._stop.wait(self.every_sec)

# ============= Google Sheets auth =============

def get_gspread_client_from_secret() -> Tuple[gspread.Client, str]:
    raw = env_str("GOOGLE_SERVICE_ACCOUNT_JSON")
    info = json.loads(raw)
    client_email = info.get("client_email", "(unknown)")
    log(f"DEBUG service account client_email = {client_email}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc, client_email

def open_ws(gc: gspread.Client, cfg: Config):
    sh = gc.open_by_key(cfg.spreadsheet_id)
    ws = sh.worksheet(cfg.worksheet_name)
    return ws

# ============= Sheet read/write helpers =============

def read_h_to_l(ws) -> List[Tuple[int, str, str, str, str, str]]:
    values = ws.get("H:L")
    if not values:
        return []
    rows = []
    for i, row in enumerate(values[1:], start=2):
        row = (row + [""] * 5)[:5]
        url, v, like, cmt, share = row
        rows.append((i, (url or "").strip(), v, like, cmt, share))
    return rows

def build_retry_targets(rows):
    targets = []
    missing_fields = 0
    missing_by_col = {"view": 0, "like": 0, "comment": 0, "share": 0}

    for r, url, v, like, cmt, share in rows:
        if not url:
            continue

        miss_v = is_empty(v)
        miss_like = is_empty(like)
        miss_cmt = is_empty(cmt)
        miss_share = is_empty(share)

        if miss_v or miss_like or miss_cmt or miss_share:
            targets.append((r, url))

        if miss_v: missing_fields += 1; missing_by_col["view"] += 1
        if miss_like: missing_fields += 1; missing_by_col["like"] += 1
        if miss_cmt: missing_fields += 1; missing_by_col["comment"] += 1
        if miss_share: missing_fields += 1; missing_by_col["share"] += 1

    return targets, missing_fields, missing_by_col

def batch_update_rows(ws, updates: Dict[int, Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]]) -> None:
    if not updates:
        return

    rows_sorted = sorted(updates.keys())
    i = 0

    while i < len(rows_sorted):
        start = rows_sorted[i]
        j = i
        while j + 1 < len(rows_sorted) and rows_sorted[j + 1] == rows_sorted[j] + 1:
            j += 1
        end = rows_sorted[j]

        cur_block = ws.get(f"I{start}:L{end}")
        block_len = end - start + 1
        cur_block = cur_block or []
        if len(cur_block) < block_len:
            cur_block = cur_block + [["", "", "", ""]] * (block_len - len(cur_block))

        out_values = []
        for offset, r in enumerate(range(start, end + 1)):
            cur_row = (cur_block[offset] + ["", "", "", ""])[:4]
            cv, cl, cc, cs = cur_row

            nv, nl, nc, ns = updates.get(r, (None, None, None, None))
            fv = cv if nv is None else nv
            fl = cl if nl is None else nl
            fc = cc if nc is None else nc
            fs = cs if ns is None else ns
            out_values.append([fv, fl, fc, fs])

        ws.update(f"I{start}:L{end}", out_values, value_input_option="RAW")
        i = j + 1

# ============= TikTok fetch (Async Playwright) =============

def extract_counts_from_html(html: str) -> Optional[Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]]:
    patterns = {
        "play": r'"playCount"\s*:\s*(\d+)',
        "like": r'"diggCount"\s*:\s*(\d+)',
        "comment": r'"commentCount"\s*:\s*(\d+)',
        "share": r'"shareCount"\s*:\s*(\d+)',
    }

    def pick_max(pattern: str) -> Optional[int]:
        nums = re.findall(pattern, html)
        if not nums:
            return None
        return max(int(x) for x in nums)

    play = pick_max(patterns["play"])
    like = pick_max(patterns["like"])
    comment = pick_max(patterns["comment"])
    share = pick_max(patterns["share"])

    if play is None and like is None and comment is None and share is None:
        return None
    return (play, like, comment, share)

async def fetch_tiktok_metrics(page, url: str, cfg: Config) -> Optional[Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]]:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=cfg.nav_timeout_ms)
        await asyncio.sleep(cfg.extra_wait_sec)
        html = await page.content()
        return extract_counts_from_html(html)
    except Exception as e:
        log(f"ERROR goto failed url={url} err={e}")
        return None

# ============= Main loop =============

async def run_until_complete(gc, cfg: Config) -> None:
    from playwright.async_api import async_playwright

    ws = open_ws(gc, cfg)
    log(f"Opened worksheet: {cfg.worksheet_name}")

    stall = 0
    prev_missing_fields: Optional[int] = None

    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=cfg.headless)
        context = await browser.new_context(
            user_agent=user_agent,
            viewport={"width": 1280, "height": 720},
            locale="en-US",
        )
        page = await context.new_page()

        try:
            for round_idx in range(1, cfg.max_rounds + 1):
                round_start = time.time()

                rows = read_h_to_l(ws)
                await asyncio.sleep(cfg.read_pause_sec)

                targets, missing_fields, missing_by_col = build_retry_targets(rows)
                log(
                    f"[Round {round_idx}] targets={len(targets)} missing_fields={missing_fields} "
                    f"(view={missing_by_col['view']}, like={missing_by_col['like']}, "
                    f"comment={missing_by_col['comment']}, share={missing_by_col['share']})"
                )

                if missing_fields == 0:
                    log("No missing fields. Done.")
                    return

                if prev_missing_fields is not None and missing_fields >= prev_missing_fields:
                    stall += 1
                    log(f"[Round {round_idx}] no improvement (stall {stall}/{cfg.stall_rounds})")
                    if stall >= cfg.stall_rounds:
                        log("Stopping due to no improvement (likely permanently missing fields).")
                        break
                else:
                    stall = 0
                prev_missing_fields = missing_fields

                pending: Dict[int, Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]] = {}
                total = len(targets)

                for idx, (row_idx, url) in enumerate(targets, start=1):
                    t0 = time.time()
                    metrics = await fetch_tiktok_metrics(page, url, cfg)
                    if metrics is not None:
                        pending[row_idx] = metrics

                    if len(pending) >= cfg.write_batch_size:
                        log(f"[Round {round_idx}] writing batch ({len(pending)} rows)")
                        batch_update_rows(ws, pending)
                        pending.clear()
                        await asyncio.sleep(cfg.write_pause_sec)

                    if idx % cfg.log_every_n == 0 or idx == total:
                        elapsed = time.time() - round_start
                        avg = elapsed / max(1, idx)
                        eta = avg * (total - idx)
                        last = time.time() - t0
                        log(
                            f"[Round {round_idx}] progress {idx}/{total} "
                            f"last={human_sec(last)} elapsed={human_sec(elapsed)} ETA={human_sec(eta)} "
                            f"buffer={len(pending)}"
                        )

                    await sleep_rand(cfg.sleep1_min, cfg.sleep1_max)

                if pending:
                    log(f"[Round {round_idx}] flushing last batch ({len(pending)} rows)")
                    batch_update_rows(ws, pending)
                    pending.clear()
                    await asyncio.sleep(cfg.write_pause_sec)

                await sleep_rand(cfg.sleep2_min, cfg.sleep2_max)

        finally:
            await context.close()
            await browser.close()

    rows = read_h_to_l(ws)
    targets, missing_fields, missing_by_col = build_retry_targets(rows)
    log(
        f"Finished. Remaining missing_fields={missing_fields} "
        f"(view={missing_by_col['view']}, like={missing_by_col['like']}, "
        f"comment={missing_by_col['comment']}, share={missing_by_col['share']}) "
        f"targets_left={len(targets)}"
    )

async def main():
    cfg = load_config()
    hb = Heartbeat(cfg.heartbeat_every_sec)
    hb.start()
    try:
        log("Starting... auth Google Sheets")
        gc, _ = get_gspread_client_from_secret()
        await run_until_complete(gc, cfg)
        log("All done.")
    finally:
        hb.stop()

if __name__ == "__main__":
    asyncio.run(main())
