import requests
import pandas as pd
from datetime import datetime, timezone
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "raw", "xrp_full_hourly_clean.csv")
KRAKEN_URL = "https://api.kraken.com/0/public/OHLC"
PAIR = "XRPUSD"
INTERVAL = 60  # minutes


def fetch_kraken_ohlc():
    params = {
        "pair": PAIR,
        "interval": INTERVAL
    }

    response = requests.get(KRAKEN_URL, params=params)
    data = response.json()

    if data.get("error"):
        raise Exception(f"Kraken API error: {data['error']}")

    # Kraken returns result with one dynamic pair key + "last"
    result_keys = [k for k in data["result"].keys() if k != "last"]
    pair_key = result_keys[0]
    result = data["result"][pair_key]

    df = pd.DataFrame(result, columns=[
        "timestamp", "open", "high", "low", "close",
        "vwap", "volume", "count"
    ])

    # Convert types
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)

    return df[["timestamp", "open", "high", "low", "close", "volume"]]


def load_existing():
    df = pd.read_csv(CSV_PATH)

    # Normalize columns
    df.columns = [c.lower() for c in df.columns]

    if "date" in df.columns:
        df.rename(columns={"date": "timestamp"}, inplace=True)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    keep_cols = ["timestamp", "open", "high", "low", "close", "volume"]
    df = df[keep_cols].copy()

    return df


def filter_new_data(existing_df, new_df):
    last_ts = existing_df["timestamp"].max()

    # Only strictly newer candles
    df = new_df[new_df["timestamp"] > last_ts].copy()

    return df


def drop_incomplete_candle(df):
    """
    Remove the most recent candle if it is not fully closed
    """
    now = datetime.now(timezone.utc)

    if df.empty:
        return df

    last_ts = df["timestamp"].iloc[-1]

    # Candle is 1 hour long → must be at least 1 hour old to be closed
    if (now - last_ts).total_seconds() < 3600:
        df = df.iloc[:-1]

    return df


def merge_and_save(existing_df, new_df):
    df = pd.concat([existing_df, new_df], ignore_index=True)

    df = df.drop_duplicates(subset=["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Save back using original column name style
    save_df = df.copy()
    save_df.rename(columns={"timestamp": "date"}, inplace=True)
    save_df.to_csv(CSV_PATH, index=False)

    return df


def main():
    print("Loading existing data...")
    existing_df = load_existing()

    print("Fetching Kraken data...")
    new_df = fetch_kraken_ohlc()

    print("Filtering new candles...")
    new_df = filter_new_data(existing_df, new_df)

    print(f"New rows before close-filter: {len(new_df)}")

    new_df = drop_incomplete_candle(new_df)

    print(f"New rows after close-filter: {len(new_df)}")

    if new_df.empty:
        print("No new closed candles. Nothing to update.")
        return

    print("Merging and saving...")
    merged = merge_and_save(existing_df, new_df)

    print(f"Update complete. Total rows: {len(merged)}")
    print(f"Latest timestamp: {merged['timestamp'].max()}")


if __name__ == "__main__":
    main()
