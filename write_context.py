"""
SPX 0DTE Daily Strike Logger — v2
Runs at 9:25am ET on trading days via GitHub Actions.

Two jobs in one:
  1. Write context.json to repo (prior SPX/SPY/VIX closes)
     → dashboard reads this to auto-populate 3 of the 4 inputs
  2. After market open (9:35 run), fetch SPX open and save strikes to Supabase

This script is called at 9:25am for context.json only.
fetch_and_save.py (existing) still runs at 9:35am for Supabase.
"""

import os, sys, json, re, requests
from datetime import date, datetime, timedelta
import pytz
import yfinance as yf

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]       # built-in Actions token
GITHUB_REPO  = os.environ.get("GITHUB_REPOSITORY", "dbhagat/SPX-0DTE")

VIX_THRESHOLDS = [12.4, 13.8, 15.0, 16.2, 17.6, 19.5, 22.2, 26.2, 32.8, 999]


def get_vix_bucket(vix):
    for i, t in enumerate(VIX_THRESHOLDS):
        if vix <= t:
            return i + 1
    return 10


def fetch_prior_closes():
    """Fetch prior day closes for SPX, SPY, VIX."""
    print("Fetching prior closes from Yahoo Finance...")
    spx = yf.Ticker("^GSPC").history(period="5d")
    spy = yf.Ticker("SPY").history(period="5d")
    vix = yf.Ticker("^VIX").history(period="5d")

    prior_spx = float(spx["Close"].iloc[-1])
    prior_spy = float(spy["Close"].iloc[-1])
    prior_vix = float(vix["Close"].iloc[-1])

    print(f"  Prior SPX close: {prior_spx:.2f}")
    print(f"  Prior SPY close: {prior_spy:.2f}")
    print(f"  Prior VIX close: {prior_vix:.2f}")

    return prior_spx, prior_spy, prior_vix


def write_context_json(prior_spx, prior_spy, prior_vix):
    """Write context.json to the repo via GitHub Contents API."""
    et      = pytz.timezone("America/New_York")
    today   = datetime.now(et).date()
    bucket  = get_vix_bucket(prior_vix)

    payload = {
        "date":            str(today),
        "prior_spx_close": round(prior_spx, 2),
        "prior_spy_close": round(prior_spy, 2),
        "prior_vix_close": round(prior_vix, 2),
        "vix_bucket":      bucket,
        "generated_at":    datetime.now(et).strftime("%Y-%m-%d %H:%M ET"),
    }

    content_b64 = __import__("base64").b64encode(
        json.dumps(payload, indent=2).encode()
    ).decode()

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/context.json"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github+json",
    }

    # Get current SHA if file exists (needed for update)
    sha = None
    r = requests.get(api_url, headers=headers)
    if r.status_code == 200:
        sha = r.json()["sha"]

    body = {
        "message": f"chore: update context.json for {today}",
        "content": content_b64,
    }
    if sha:
        body["sha"] = sha

    resp = requests.put(api_url, headers=headers, json=body)
    if resp.status_code in (200, 201):
        print(f"✅ context.json written for {today} (bucket {bucket}, VIX {prior_vix:.2f})")
    else:
        print(f"❌ GitHub API error {resp.status_code}: {resp.text}")
        sys.exit(1)


def main():
    et = pytz.timezone("America/New_York")
    today = datetime.now(et).date()

    if today.weekday() >= 5:
        print(f"Skipping — {today} is a weekend.")
        sys.exit(0)

    print(f"=== Context Writer — {today} ===")
    prior_spx, prior_spy, prior_vix = fetch_prior_closes()
    write_context_json(prior_spx, prior_spy, prior_vix)


if __name__ == "__main__":
    main()
