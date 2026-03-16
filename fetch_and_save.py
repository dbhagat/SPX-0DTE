"""
SPX 0DTE Daily Strike Logger
Runs at 9:35am ET on trading days via GitHub Actions.
Fetches SPX open + prior closes, calculates 98/99/100% win-rate strikes,
saves to Supabase daily_strikes table.
"""

import os, sys, re, json, time
from datetime import date, datetime, timedelta
import pytz
import yfinance as yf
import pandas as pd
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

VIX_THRESHOLDS = [12.4, 13.8, 15.0, 16.2, 17.6, 19.5, 22.2, 26.2, 32.8, 999]


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


def fetch_with_retry(ticker_sym, period="5d", retries=4, delay=15):
    """
    Fetch ticker history with retries and exponential backoff.
    Uses yf.download() which is more reliable than .history() for rate limits.
    """
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(
                ticker_sym,
                period=period,
                interval="1d",
                auto_adjust=True,
                progress=False,
            )
            if df is not None and not df.empty:
                return df
            raise ValueError(f"Empty data for {ticker_sym}")
        except Exception as e:
            if attempt == retries:
                raise
            wait = delay * attempt
            print(f"  Attempt {attempt} failed for {ticker_sym}: {e} — retrying in {wait}s...")
            time.sleep(wait)


def fetch_market_data():
    """Fetch SPX open + prior closes with retry logic."""
    print("Fetching market data...")

    # Stagger requests to avoid simultaneous rate limiting
    spx_df = fetch_with_retry("^GSPC")
    time.sleep(3)
    spy_df = fetch_with_retry("SPY")
    time.sleep(3)
    vix_df = fetch_with_retry("^VIX")

    # Flatten MultiIndex columns if present (yf.download returns MultiIndex)
    def get_col(df, col):
        if isinstance(df.columns, pd.MultiIndex):
            return df[col].iloc[:, 0]
        return df[col]

    spx_open        = float(get_col(spx_df, "Open").iloc[-1])
    prior_spx_close = float(get_col(spx_df, "Close").iloc[-2])
    prior_spy_close = float(get_col(spy_df, "Close").iloc[-2])
    prior_vix_close = float(get_col(vix_df, "Close").iloc[-2])

    print(f"  SPX Open:        {spx_open:.2f}")
    print(f"  Prior SPX Close: {prior_spx_close:.2f}")
    print(f"  Prior SPY Close: {prior_spy_close:.2f}")
    print(f"  Prior VIX Close: {prior_vix_close:.2f}")

    return spx_open, prior_spx_close, prior_spy_close, prior_vix_close


def calculate_strikes(spx_open, prior_spx_close, prior_spy_close, prior_vix_close):
    bucket_data = load_bucket_data()
    bucket = get_vix_bucket(prior_vix_close)
    arr = bucket_data[bucket]

    results = {}
    for level in [98, 99, 100]:
        wr = 0.9999 if level == 100 else level / 100
        put_pct, call_pct = get_percentile(arr, wr)
        results[level] = {
            "put_strike":  fmt_strike(spx_open, put_pct),
            "call_strike": fmt_strike(spx_open, call_pct),
        }
        print(f"  {level}%  Put: {results[level]['put_strike']}  Call: {results[level]['call_strike']}")

    return bucket, results


def save_to_supabase(today, spx_open, prior_spx_close, prior_spy_close, prior_vix_close, bucket, strikes):
    url = f"{SUPABASE_URL}/rest/v1/daily_strikes"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }
    payload = {
        "date":             str(today),
        "spx_open":         round(spx_open, 2),
        "prior_spx_close":  round(prior_spx_close, 2),
        "prior_spy_close":  round(prior_spy_close, 2),
        "prior_vix_close":  round(prior_vix_close, 2),
        "vix_bucket":       bucket,
        "put_98":           strikes[98]["put_strike"],
        "call_98":          strikes[98]["call_strike"],
        "put_99":           strikes[99]["put_strike"],
        "call_99":          strikes[99]["call_strike"],
        "put_100":          strikes[100]["put_strike"],
        "call_100":         strikes[100]["call_strike"],
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code in (200, 201):
        print(f"✅ Saved to Supabase for {today}")
    else:
        print(f"❌ Supabase error {resp.status_code}: {resp.text}")
        sys.exit(1)


def main():
    et = pytz.timezone("America/New_York")
    today = datetime.now(et).date()

    if today.weekday() >= 5:
        print(f"Skipping — {today} is a weekend.")
        sys.exit(0)

    print(f"=== SPX 0DTE Strike Logger — {today} ===")
    spx_open, prior_spx_close, prior_spy_close, prior_vix_close = fetch_market_data()
    bucket, strikes = calculate_strikes(spx_open, prior_spx_close, prior_spy_close, prior_vix_close)
    save_to_supabase(today, spx_open, prior_spx_close, prior_spy_close, prior_vix_close, bucket, strikes)


if __name__ == "__main__":
    main()
