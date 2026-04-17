import os
from google.cloud import storage

BASE_DIR = os.path.expanduser("~/projects/xrp-grid-brain")
SNAPSHOT_DIR = os.path.join(BASE_DIR, "outputs", "snapshot")

# 👇 CHANGE THIS
BUCKET_NAME = "financial_databucket"

client = storage.Client.from_service_account_json(
    os.path.join(BASE_DIR, "credentials/google_sheets_service_account.json")
)

bucket = client.bucket(BUCKET_NAME)


def upload_file(filepath):
    filename = os.path.basename(filepath)
    blob = bucket.blob(f"snapshots/{filename}")

    blob.upload_from_filename(filepath)

    print(f"Uploaded {filename} → gs://{BUCKET_NAME}/snapshots/{filename}")


def main():
    files = [
        "system_state.json",
        "repo_map.txt",
        "context.txt",
    ]

    for fname in files:
        path = os.path.join(SNAPSHOT_DIR, fname)
        if os.path.exists(path):
            upload_file(path)
        else:
            print(f"Missing: {fname}")


if __name__ == "__main__":
    main()
