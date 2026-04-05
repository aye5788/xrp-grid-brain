import pandas as pd

def load_ohlcv_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # Normalize column names
    df.columns = [c.lower() for c in df.columns]

    if "date" in df.columns:
        df.rename(columns={"date": "timestamp"}, inplace=True)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    required = ["timestamp", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    return df[required]
