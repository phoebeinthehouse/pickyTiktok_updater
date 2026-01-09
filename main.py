import os, re, json, time, random
from datetime import datetime, timezone

import requests
import gspread
from google.oauth2.service_account import Credentials

# ===== Your sheet settings =====
SPREADSHEET_NAME = os.environ.get("SPREADSHEET_NAME", "tiktok_raw_gitTestVer")
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "Sheet1")

URL_COL = 8            # H
OUT_START_COL = 9      # I
OUT_END_COL = 12       # L (views, likes, comments, shares)

# ===== Speed/Retry =====
SLEEP1_MIN = float(os.environ.get("SLEEP1_MIN", "2.0"))
SLEEP1_MAX = float(os.environ.get("SLEEP1_MAX", "4.0"))
MAX_RETRIES1 = int(os.environ.get("MAX_RETRIES1", "3"))
BACKOFF1 = float(os.environ.get("BACKOFF1", "2.0"))

SLEEP2_MIN = float(os.environ.get("SLEEP2_MIN", "4.0"))
SLEEP2_MAX = float(os.environ.get("SLEEP2_MAX", "7.0"))
MAX_RETRIES2 = int(os.environ.get("MAX_RETRIES2", "5"))
BACKOFF2 = float(os.environ.get("BACKOFF2", "2.2"))

WRITE_BLOCK_ROWS = int(os.environ.get("WRITE_BLOCK_ROWS", "200"))

# ===== GitHub Actions logging =====
LOG_EVERY_N = int(os.environ.get("LOG_EVERY_N", "25"))
HEARTBEAT_EVERY_SEC = int(os.environ.get("HEARTBEAT_EVERY_SEC", "120"))

def utc_now_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def fmt_seconds(sec: float) -> str:
    sec = max(0, int(sec))
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def col_to_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def a1_range(row_start, row_end, col_start, col_end):
    return f"{col_to_letter(col_start)}{row_start}:{col_to_letter(col_end)}{row_end}"

def is_short_or_redirect_url(url: str) -> bool:
    u = (url or "").lower()
    return ("vm.tiktok.com" in u) or ("vt.tiktok.com" in u) or ("/t/" in u)

def get_gspread_client():
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    info = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def resolve_final_url(session: requests.Session, url: str, timeout: int = 20) -> str:
    if not is_short_or_redirect_url(url):
        return url
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = session.head(url, allow_redirects=True, timeout=timeout, headers=headers)
        return r.url
    except Exception:
        r = session.get(url, allow_redirects=True, timeout=timeout, headers=headers)
        return r.url

def fetch_html(session: requests.Session, url: str, timeout: int = 25) -> str:
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}
    r = session.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def extract_stats_from_any_json(obj):
    target_keys = {"playCount", "diggCount", "commentCount", "shareCount"}

    def walk(x):
        if isinstance(x, dict):
            if target_keys.issubset(set(x.keys())):
                return x
            for v in x.values():
                found = walk(v)
                if found:
                    return found
        elif isinstance(x, list):
            for item in x:
                found = walk(item)
                if found:
                    return found
        return None

    stats = walk(obj)
    if not stats:
        return None

    return (
        stats.get("playCount"),
        stats.get("diggCount"),
        stats.get("commentCount"),
        stats.get("shareCount"),
    )

def parse_stats(html: str):
    for script_id in ["SIGI_STATE", "__UNIVERSAL_DATA_FOR_REHYDRATION__", "__NEXT_DATA__"]:
        m = re.search(rf'<script id="{script_id}" type="application/json">(.*?)</script>', html, re.DOTALL)
        if m:
            data = json.loads(m.group(1))
            return extract_stats_from_any_json(data)
    return None

def fetch_stats_with_retry(session: requests.Session, url: str, max_retries: int, backoff_base: float):
    for attempt in range(max_retries + 1):
        try:
            final_url = resolve_final_url(session, url)
            html = fetch_html(session, final_url)
            return parse_stats(html)
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code in (403, 429, 500, 502, 503, 504) and attempt < max_retries:
                time.sleep((backoff_base ** attempt) + random.uniform(0.5, 1.5))
                continue
            return None
        except Exception:
            if attempt < max_retries:
                time.sleep((backoff_base ** attempt) + random.uniform(0.5, 1.5))
                continue
            return None

def write_block(ws, start_row: int, block_values: list):
    if not block_values:
        return
    end_row = start_row + len(block_values) - 1
    rng = a1_range(start_row, end_row, OUT_START_COL, OUT_END_COL)
    ws.update(rng, block_values)

def write_all_out(ws, start_row: int, out: list):
    total = len(out)
    for offset in range(0, total, WRITE_BLOCK_ROWS):
        block = out[offset: offset + WRITE_BLOCK_ROWS]
        write_block(ws, start_row + offset, block)
        time.sleep(random.uniform(1.0, 2.0))

def write_sparse_updates(ws, updates: list):
    if not updates:
        return
    updates.sort(key=lambda x: x[0])

    block_start = updates[0][0]
    block_vals = [updates[0][1]]
    prev = block_start

    for row_idx, vals in updates[1:]:
        if row_idx == prev + 1:
            block_vals.append(vals)
            prev = row_idx
        else:
            write_block(ws, block_start, block_vals)
            time.sleep(random.uniform(1.0, 2.0))
            block_start, prev, block_vals = row_idx, row_idx, [vals]

    write_block(ws, block_start, block_vals)

def progress_line(tag: str, done: int, total: int, start_ts: float) -> str:
    elapsed = time.time() - start_ts
    avg = elapsed / done if done else 0
    remaining = avg * (total - done) if done else 0
    pct = (done / total * 100) if total else 0
    return (
        f"[{tag}] {done}/{total} ({pct:.1f}%) | "
        f"elapsed {fmt_seconds(elapsed)} | ETA {fmt_seconds(remaining)} | avg {avg:.2f}s/item"
    )

def main():
    job_start = time.time()
    print(f"[START] {utc_now_str()} | crawling + sheet update")
    print(f"[CONFIG] sheet='{SPREADSHEET_NAME}' tab='{WORKSHEET_NAME}' | H(url) -> I-L(metrics)")

    gc = get_gspread_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(WORKSHEET_NAME)

    session = requests.Session()

    urls = ws.col_values(URL_COL)  # includes header
    if len(urls) < 2:
        print("[EXIT] No URLs found in column H (need from row 2).")
        return

    start_row = 2
    end_row = len(urls)
    total = end_row - start_row + 1
    print(f"[INFO] total rows to process: {total} (rows {start_row}-{end_row})")

    # PASS 1
    pass1_start = time.time()
    out = []
    failed_rows = []
    last_heartbeat = time.time()

    for i, row_idx in enumerate(range(start_row, end_row + 1), start=1):
        url = (urls[row_idx - 1] or "").strip()

        if not url:
            out.append(["", "", "", ""])
            failed_rows.append(row_idx)
        else:
            stats = fetch_stats_with_retry(session, url, MAX_RETRIES1, BACKOFF1)
            if not stats:
                out.append(["", "", "", ""])
                failed_rows.append(row_idx)
            else:
                v, l, c, s = stats
                out.append([str(v or ""), str(l or ""), str(c or ""), str(s or "")])

        if i % LOG_EVERY_N == 0 or i == total:
            print(progress_line("PASS1", i, total, pass1_start))

        now = time.time()
        if now - last_heartbeat >= HEARTBEAT_EVERY_SEC:
            print(f"[HEARTBEAT] {utc_now_str()} | " + progress_line("PASS1", i, total, pass1_start))
            last_heartbeat = now

        time.sleep(random.uniform(SLEEP1_MIN, SLEEP1_MAX))

    print(f"[PASS1] done in {fmt_seconds(time.time()-pass1_start)} | failures={len(failed_rows)}")
    print(f"[SHEETS] writing PASS1 results (block={WRITE_BLOCK_ROWS}) ...")
    write_all_out(ws, start_row, out)
    print("[SHEETS] PASS1 write complete")

    # PASS 2
    if not failed_rows:
        print(f"[DONE] {utc_now_str()} | total runtime {fmt_seconds(time.time()-job_start)}")
        return

    pass2_total = len(failed_rows)
    pass2_start = time.time()
    retry_updates = []
    retry_success = 0
    last_heartbeat = time.time()

    for j, row_idx in enumerate(failed_rows, start=1):
        url = (urls[row_idx - 1] or "").strip()
        if url:
            stats = fetch_stats_with_retry(session, url, MAX_RETRIES2, BACKOFF2)
            if stats:
                v, l, c, s = stats
                retry_updates.append((row_idx, [str(v or ""), str(l or ""), str(c or ""), str(s or "")]))
                retry_success += 1

        if j % LOG_EVERY_N == 0 or j == pass2_total:
            print(progress_line("PASS2", j, pass2_total, pass2_start) + f" | pass2_success={retry_success}")

        now = time.time()
        if now - last_heartbeat >= HEARTBEAT_EVERY_SEC:
            print(f"[HEARTBEAT] {utc_now_str()} | " + progress_line("PASS2", j, pass2_total, pass2_start))
            last_heartbeat = now

        time.sleep(random.uniform(SLEEP2_MIN, SLEEP2_MAX))

    print(f"[PASS2] done in {fmt_seconds(time.time()-pass2_start)} | success={retry_success}/{pass2_total}")

    if retry_updates:
        print(f"[SHEETS] writing PASS2 successes (merged blocks). rows={len(retry_updates)}")
        write_sparse_updates(ws, retry_updates)
        print("[SHEETS] PASS2 write complete")

    print(f"[DONE] {utc_now_str()} | total runtime {fmt_seconds(time.time()-job_start)}")

if __name__ == "__main__":
    main()
