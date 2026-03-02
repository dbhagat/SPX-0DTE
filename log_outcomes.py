"""
SPX 0DTE Outcome Logger - runs 4:15pm ET daily and on manual trigger.
WIN = SPX stayed inside the strikes all day AND closed inside.
  breach_intraday = (Low < put_strike) OR (High > call_strike)
  breach_close    = (Close < put_strike) OR (Close > call_strike)
"""

import os, sys, time, requests
import yfinance as yf
import pandas as pd
from datetime import date, timedelta

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
}

def get_unfilled_rows():
    today = str(date.today())
    url = (f"{SUPABASE_URL}/rest/v1/daily_strikes"
           f"?select=id,date,put_98,call_98,put_99,call_99,put_100,call_100"
           f"&or=(outcome_filled.is.null,outcome_filled.eq.false)"
           f"&date=lt.{today}&order=date.asc&limit=500")
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"Fetch error {resp.status_code}: {resp.text}"); sys.exit(1)
    return resp.json()

def fetch_ohlc(start_str, end_str):
    s = (date.fromisoformat(start_str) - timedelta(days=3)).isoformat()
    e = (date.fromisoformat(end_str)   + timedelta(days=2)).isoformat()
    print(f"Fetching SPX OHLC: {s} to {e}")
    hist = yf.Ticker("^GSPC").history(start=s, end=e)
    hist.index = pd.to_datetime(hist.index).date
    print(f"  {len(hist)} days")
    return hist

def calc(low, high, close, put_k, call_k):
    pi = low   < put_k;  ci = high  > call_k
    pc = close < put_k;  cc = close > call_k
    intra = pi or ci;    at_close = pc or cc
    sides = (["put"] if pi or pc else []) + (["call"] if ci or cc else [])
    side = "both" if len(sides)==2 else (sides[0] if sides else None)
    return intra, at_close, side

def patch(row_id, payload):
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/daily_strikes?id=eq.{row_id}",
                       headers=HEADERS, json=payload)
    return r.status_code in (200, 204)

def main():
    rows = get_unfilled_rows()
    print(f"Rows to fill: {len(rows)}")
    if not rows: print("All done."); return

    dates = [r["date"] for r in rows]
    hist  = fetch_ohlc(dates[0], dates[-1])
    ok = bad = skip = 0

    for r in rows:
        d = date.fromisoformat(r["date"])
        if d not in hist.index: print(f"  {r['date']} no data"); skip+=1; continue
        o = hist.loc[d]
        H,L,C = float(o["High"]), float(o["Low"]), float(o["Close"])
        res = {lvl: calc(L,H,C, r[f"put_{lvl}"], r[f"call_{lvl}"]) for lvl in [98,99,100]}
        payload = {
            "spx_high": round(H,2), "spx_low": round(L,2), "spx_close": round(C,2),
            **{f"breach_intraday_{l}": res[l][0] for l in [98,99,100]},
            **{f"breach_close_{l}":    res[l][1] for l in [98,99,100]},
            **{f"breach_side_{l}":     res[l][2] for l in [98,99,100]},
            "outcome_filled": True,
        }
        icon = lambda l: "WIN" if (not res[l][0] and not res[l][1]) else f"LOSS-{res[l][2] or '?'}"
        if patch(r["id"], payload):
            print(f"  {r['date']}  H:{H:.0f} L:{L:.0f} C:{C:.0f}  98:{icon(98)}  99:{icon(99)}  100:{icon(100)}")
            ok+=1
        else:
            print(f"  {r['date']} PATCH FAILED"); bad+=1
        time.sleep(0.05)

    print(f"\nDone: {ok} updated / {skip} skipped / {bad} errors")

if __name__ == "__main__":
    main()
