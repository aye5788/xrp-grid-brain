import requests
import pandas as pd
from datetime import datetime

CSV_PATH = "data/raw/xrp_full_hourly_clean.csv"

KRAKEN_URL = "https://api.kraken.com/0/public/OHLC"
PAIR = "XRPUSD"
INTERVAL = 60


def fetch_batch(since=None):
    params = {
        "pair": PAIR,
        "interval": INTERVAL
    }
    if since:
        params["since"] = since

    r = requests.get(KRAKEN_URL, params=params)
    data = r.json()

    result_keys = [k for k in data["result"].keys() if k != "last"]
    pair_key = result_keys[0]
    rows = data["result"][pair_key]

    df = pd.DataFrame(rows, columns=[
        "timestamp", "open", "high", "low", "close",
        "vwap", "volume", "count"
    ])

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]

    df = df.astype({
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float
    })

    return df, data["result"]["last"]


def main():
    print("Bootstrapping XRP data...")

    all_dfs = []
    since = None

    for i in range(100):  # ~100 batches = decent history
        df, since = fetch_batch(since)
        if df.empty:
            break

        all_dfs.append(df)

        print(f"Batch {i+1}, rows: {len(df)}")

    full_df = pd.concat(all_dfs).drop_duplicates(subset=["timestamp"])
    full_df = full_df.sort_values("timestamp")

    full_df.rename(columns={"timestamp": "date"}, inplace=True)
    full_df.to_csv(CSV_PATH, index=False)

    print(f"Saved to {CSV_PATH}, total rows: {len(full_df)}")


if __name__ == "__main__":
    main()
