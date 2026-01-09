import os
import re
import json
import time
import random
from datetime import datetime, timezone

import requests
import gspread
from google.oauth2.service_account import Credentials


# =====================
# BASIC CONFIG
# =====================
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "Sheet1")
SPREADSHEET_ID = os.environ.get(
    "SPREADSHEET_ID",
    "1vJ-BXCor7tfYHsez62Sg-LgBF1tJEzDJmhTSABpR6fI"
)

URL_COL = 8        # H
OUT_START_COL = 9  # I
OUT_END_COL = 12   # L

# Speed / retry
SLEEP1_MIN = float(os.environ.get("SLEEP1_MIN", "2.0"))
SLEEP1_MAX = float(os.environ.get("SLEEP1_MAX", "4.0"))
SLEEP2_MIN = float(os.environ.get("SLEEP2_MIN", "4.0"))
SLEEP2_MAX = float(os.environ.get("SLEEP2_MAX", "7.0"))

MAX_RETRIES1 = 3
MAX_RETRIES2 = 5

LOG_EVERY_N = int(os.environ.get("LOG_EVERY_N", "25"))
HEARTBEAT_EVERY_SEC = int(os.environ.get("HEARTBEAT_EVERY_SEC", "120"))
WRITE_BLOCK_ROWS = int(os.environ.get("WRITE_BLOCK_ROWS", "200"))


# =====================
# UTILS
# =====================
def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def col_letter(n):
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def a1_range(r1, r2, c1, c2):
    return f"{col_letter(c1)}{r1}:{col_letter(c2)}{r2}"


def fmt(sec):
    sec = int(max(sec, 0))
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


# =====================
# GOOGLE AUTH
# =====================
def get_gspread_client():
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    info = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# =====================
# TIKTOK PARSING
# =====================
def resolve_url(session, url):
    if "vm.tiktok.com" not in url and "vt.tiktok.com" not in url:
        return url
    try:
        r = session.head(url, allow_redirects=True, timeout=15)
        return r.url
    except Exception:
        r = session.get(url, allow_redirects=True, timeout=15)
        return r.url


def fetch_html(session, url):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = session.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text


def extract_stats(html):
    for sid in ["SIGI_STATE", "__UNIVERSAL_DATA_FOR_REHYDRATION__", "__NEXT_DATA__"]:
        m = re.search(
            rf'<script id="{sid}" type="application/json">(.*?)</script>',
            html,
            re.DOTALL,
        )
        if not m:
            continue

        data = json.loads(m.group(1))

        def walk(x):
            if isinstance(x, dict):
                if {"playCount", "diggCount", "commentCount", "shareCount"} <= set(x):
                    return x
                for v in x.values():
                    r = walk(v)
                    if r:
                        return r
            elif isinstance(x, list):
                for i in x:
                    r = walk(i)
                    if r:
                        return r
            return None

        stats = walk(data)
        if stats:
            return (
                stats.get("playCount"),
                stats.get("diggCount"),
                stats.get("commentCount"),
                stats.get("shareCount"),
            )

    return None


def fetch_with_retry(session, url, max_retry):
    for i in range(max_retry):
        try:
            final = resolve_url(session, url)
            html = fetch_html(session, final)
            return extract_stats(html)
        except Exception:
            time.sleep(2 ** i + random.random())
    return None


# =====================
# MAIN
# =====================
def main():
    print(f"[START] {utc_now()}")

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    session = requests.Session()

    urls = ws.col_values(URL_COL)
    if len(urls) < 2:
        print("[EXIT] No URLs found")
        return

    start_row = 2
    end_row = len(urls)
    total = end_row - start_row + 1

    print(f"[INFO] Total URLs: {total}")

    out = []
    failed = []

    t0 = time.time()
    last_hb = time.time()

    # -------- PASS 1 --------
    for idx, row in enumerate(range(start_row, end_row + 1), start=1):
        url = urls[row - 1].strip()

        if not url:
            out.append(["", "", "", ""])
            failed.append(row)
        else:
            stats = fetch_with_retry(session, url, MAX_RETRIES1)
            if not stats:
                out.append(["", "", "", ""])
                failed.append(row)
            else:
                out.append([str(x or "") for x in stats])

        if idx % LOG_EVERY_N == 0 or idx == total:
            elapsed = time.time() - t0
            avg = elapsed / idx
            eta = avg * (total - idx)
            print(f"[PASS1] {idx}/{total} | ETA {fmt(eta)}")

        if time.time() - last_hb > HEARTBEAT_EVERY_SEC:
            print(f"[HEARTBEAT] still running | {idx}/{total}")
            last_hb = time.time()

        time.sleep(random.uniform(SLEEP1_MIN, SLEEP1_MAX))

    # write pass1
    for i in range(0, len(out), WRITE_BLOCK_ROWS):
        block = out[i : i + WRITE_BLOCK_ROWS]
        r1 = start_row + i
        r2 = r1 + len(block) - 1
        ws.update(a1_range(r1, r2, OUT_START_COL, OUT_END_COL), block)
        time.sleep(1)

    print(f"[PASS1 DONE] failures: {len(failed)}")

    # -------- PASS 2 --------
    if failed:
        retry_ok = 0
        for i, row in enumerate(failed, start=1):
            url = urls[row - 1].strip()
            if not url:
                continue

            stats = fetch_with_retry(session, url, MAX_RETRIES2)
            if stats:
                ws.update(
                    a1_range(row, row, OUT_START_COL, OUT_END_COL),
                    [[str(x or "") for x in stats]],
                )
                retry_ok += 1

            time.sleep(random.uniform(SLEEP2_MIN, SLEEP2_MAX))

        print(f"[PASS2 DONE] recovered {retry_ok}/{len(failed)}")

    print(f"[DONE] {utc_now()}")


if __name__ == "__main__":
    main()
