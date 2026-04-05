from src.pipelines.run_brain import run_pipeline
from src.pipelines.run_grid import generate_latest_grid

CSV_PATH = "data/raw/xrp_full_hourly_clean.csv"


def main():
    df = run_pipeline(CSV_PATH)

    latest = generate_latest_grid(df)

    print("\n=== LATEST DECISION ===")
    for k, v in latest.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
