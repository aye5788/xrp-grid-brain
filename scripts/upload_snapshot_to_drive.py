import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

BASE_DIR = os.path.expanduser("~/projects/xrp-grid-brain")
SNAPSHOT_DIR = os.path.join(BASE_DIR, "outputs", "snapshot")
SERVICE_ACCOUNT_FILE = os.path.join(
    BASE_DIR, "credentials", "google_sheets_service_account.json"
)

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
FOLDER_ID = "1IK9GbOVwgIRPJdP2hAAc9mLh2WMp64iu"

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES,
)

service = build("drive", "v3", credentials=creds)


def upload_file(filepath: str) -> None:
    filename = os.path.basename(filepath)

    file_metadata = {
        "name": filename,
        "parents": [FOLDER_ID],
    }

    media = MediaFileUpload(filepath, resumable=True)

    created = (
        service.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id, name",
        )
        .execute()
    )

    print(f"Uploaded {created['name']} -> file_id={created['id']}")


def main() -> None:
    target_files = [
        "system_state.json",
        "repo_map.txt",
        "context.txt",
    ]

    for fname in target_files:
        path = os.path.join(SNAPSHOT_DIR, fname)
        if os.path.exists(path):
            upload_file(path)
        else:
            print(f"Missing file, skipped: {path}")


if __name__ == "__main__":
    main()
