"""
SPX 0DTE Historical Backfill Script
Fetches open/close data for each trading day in a date range and
inserts into Supabase daily_strikes table.

Run via GitHub Actions workflow_dispatch with date range inputs,
or manually: python backfill_strikes.py 2025-01-01 2026-03-01
"""

import os
import sys
import re
import json
import time
from datetime import date, datetime, timedelta
import pytz
import yfinance as yf
import requests
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL      = os.environ["SUPABASE_URL"]
SUPABASE_KEY      = os.environ["SUPABASE_SERVICE_KEY"]
VIX_THRESHOLDS    = [12.4, 13.8, 15.0, 16.2, 17.6, 19.5, 22.2, 26.2, 32.8, 999]

# ── Load bucket data from index.html ─────────────────────────────────────────
def load_bucket_data():
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path, "r") as f:
        html = f.read()
    match = re.search(r"const BUCKET_SPX\s*=\s*(\{[\s\S]+?\n\};)", html)
    if not match:
        raise ValueError("Could not find BUCKET_SPX in index.html")
    raw = match.group(1).rstrip(";")
    raw = re.sub(r"\b(\d+):", r'"\1":', raw)
    raw = re.sub(r",\s*\}", "}", raw)
    raw = re.sub(r",\s*\]", "]", raw)
    return {int(k): v for k, v in json.loads(raw).items()}


def get_vix_bucket(vix):
    for i, thresh in enumerate(VIX_THRESHOLDS):
        if vix <= thresh:
            return i + 1
    return 10


def get_percentile(arr, win_rate):
    n = len(arr)
    put_idx  = max(0, int((1 - win_rate) * n) - 1)
    call_idx = min(n - 1, int(win_rate * n))
    return arr[put_idx], arr[call_idx]


def fmt_strike(spx_open, pct):
    return round(spx_open * (1 + pct) / 5) * 5


def get_existing_dates():
    """Fetch dates already in the DB to avoid duplicates."""
    url = f"{SUPABASE_URL}/rest/v1/daily_strikes?select=date&order=date.asc"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return {row["date"] for row in resp.json()}
    return set()


def save_row(row):
    url = f"{SUPABASE_URL}/rest/v1/daily_strikes"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }
    resp = requests.post(url, headers=headers, json=row)
    return resp.status_code in (200, 201)


def generate_weekdays(start_str, end_str):
    start = date.fromisoformat(start_str)
    end   = date.fromisoformat(end_str)
    days  = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def main():
    start_str = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    end_str   = sys.argv[2] if len(sys.argv) > 2 else "2026-03-01"

    print(f"=== Backfill: {start_str} → {end_str} ===")

    # Load bucket data once
    bucket_data = load_bucket_data()
    print(f"Loaded {len(bucket_data)} VIX buckets from index.html")

    # Get already-saved dates
    existing = get_existing_dates()
    print(f"Already in DB: {len(existing)} dates")

    # Get all weekdays in range
    trading_days = generate_weekdays(start_str, end_str)
    to_process   = [d for d in trading_days if str(d) not in existing]
    print(f"Weekdays in range: {len(trading_days)} | To process: {len(to_process)}")

    if not to_process:
        print("Nothing to backfill — all dates already in DB.")
        return

    # Fetch full history in one shot (much faster than per-day requests)
    print("\nFetching full price history from Yahoo Finance...")
    # Add buffer days before start to get prior-day closes for first date
    fetch_start = (to_process[0] - timedelta(days=10)).isoformat()
    fetch_end   = (to_process[-1] + timedelta(days=1)).isoformat()

    spx_hist = yf.Ticker("^GSPC").history(start=fetch_start, end=fetch_end)
    spy_hist = yf.Ticker("SPY").history(start=fetch_start, end=fetch_end)
    vix_hist = yf.Ticker("^VIX").history(start=fetch_start, end=fetch_end)

    # Normalize index to date strings
    spx_hist.index = pd.to_datetime(spx_hist.index).date
    spy_hist.index = pd.to_datetime(spy_hist.index).date
    vix_hist.index = pd.to_datetime(vix_hist.index).date

    spx_dates = sorted(spx_hist.index)
    print(f"Got {len(spx_dates)} trading days from Yahoo ({spx_dates[0]} → {spx_dates[-1]})")

    # Process each day
    saved = 0
    skipped = 0
    errors = 0

    for target_date in to_process:
        # Skip if not a real trading day (holiday) — won't be in spx_dates
        if target_date not in spx_hist.index:
            print(f"  {target_date} — no data (holiday/weekend), skipping")
            skipped += 1
            continue

        # Find the prior trading day
        prior_idx = spx_dates.index(target_date) - 1
        if prior_idx < 0:
            print(f"  {target_date} — no prior day data, skipping")
            skipped += 1
            continue

        prior_date = spx_dates[prior_idx]

        # Check we have all required data
        if prior_date not in spy_hist.index or prior_date not in vix_hist.index:
            print(f"  {target_date} — missing SPY/VIX prior close, skipping")
            skipped += 1
            continue

        spx_open        = float(spx_hist.loc[target_date, "Open"])
        prior_spx_close = float(spx_hist.loc[prior_date,  "Close"])
        prior_spy_close = float(spy_hist.loc[prior_date,  "Close"])
        prior_vix_close = float(vix_hist.loc[prior_date,  "Close"])

        # Calculate strikes
        bucket = get_vix_bucket(prior_vix_close)
        arr    = bucket_data[bucket]

        strikes = {}
        for level in [98, 99, 100]:
            wr = 0.9999 if level == 100 else level / 100
            put_pct, call_pct = get_percentile(arr, wr)
            strikes[level] = {
                "put":  fmt_strike(spx_open, put_pct),
                "call": fmt_strike(spx_open, call_pct),
            }

        row = {
            "date":            str(target_date),
            "spx_open":        round(spx_open, 2),
            "prior_spx_close": round(prior_spx_close, 2),
            "prior_spy_close": round(prior_spy_close, 2),
            "prior_vix_close": round(prior_vix_close, 2),
            "vix_bucket":      bucket,
            "put_98":          strikes[98]["put"],
            "call_98":         strikes[98]["call"],
            "put_99":          strikes[99]["put"],
            "call_99":         strikes[99]["call"],
            "put_100":         strikes[100]["put"],
            "call_100":        strikes[100]["call"],
        }

        if save_row(row):
            print(f"  ✅ {target_date} | VIX {prior_vix_close:.1f} (B{bucket}) | "
                  f"98%: {strikes[98]['put']}/{strikes[98]['call']} | "
                  f"99%: {strikes[99]['put']}/{strikes[99]['call']} | "
                  f"100%: {strikes[100]['put']}/{strikes[100]['call']}")
            saved += 1
        else:
            print(f"  ❌ {target_date} — Supabase write failed")
            errors += 1

        # Small delay to avoid rate limiting
        time.sleep(0.1)

    print(f"\n=== Done: {saved} saved, {skipped} skipped, {errors} errors ===")


if __name__ == "__main__":
    main()
