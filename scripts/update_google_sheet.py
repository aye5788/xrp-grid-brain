import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# -----------------------------
# CONFIG
# -----------------------------
SERVICE_ACCOUNT_FILE = "credentials/google_sheets_service_account.json"
SPREADSHEET_NAME = "XRP Grid Brain Monitor"

SHEET_CONFIG = [
    ("Sheet1", "outputs/latest_decision.csv"),
    ("Evaluation", "outputs/evaluation_history.csv"),
    ("Summary", "outputs/eval_summary_latest.csv"),
]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# -----------------------------
# AUTH
# -----------------------------
def get_client():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )
    return gspread.authorize(creds)


# -----------------------------
# HELPERS
# -----------------------------
def load_csv(path):
    if not os.path.exists(path):
        print(f"Skipping missing file: {path}")
        return None

    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"Failed reading {path}: {e}")
        return None

    if df.empty:
        print(f"Skipping empty file: {path}")
        return None

    return df


def get_or_create_worksheet(spreadsheet, title, rows=1000, cols=50):
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
    return ws


def upload_df_to_sheet(spreadsheet, sheet_name, df):
    ws = get_or_create_worksheet(
        spreadsheet,
        sheet_name,
        rows=max(1000, len(df) + 10),
        cols=max(50, len(df.columns) + 5)
    )

    ws.clear()

    values = [df.columns.tolist()] + df.astype(str).values.tolist()
    ws.update(values)

    print(f"Updated Google Sheet tab: {sheet_name}")


# -----------------------------
# MAIN
# -----------------------------
def main():
    client = get_client()
    spreadsheet = client.open(SPREADSHEET_NAME)

    for sheet_name, csv_path in SHEET_CONFIG:
        df = load_csv(csv_path)
        if df is not None:
            upload_df_to_sheet(spreadsheet, sheet_name, df)

    print(f"Google Sheet updated successfully: {SPREADSHEET_NAME}")


if __name__ == "__main__":
    main()
