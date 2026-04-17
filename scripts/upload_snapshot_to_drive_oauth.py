import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

BASE_DIR = os.path.expanduser("~/projects/xrp-grid-brain")
SNAPSHOT_DIR = os.path.join(BASE_DIR, "outputs", "snapshot")

# Put your OAuth client JSON here after downloading it from Google Cloud Console
CLIENT_SECRET_FILE = os.path.join(BASE_DIR, "credentials", "google_drive_oauth_client.json")
TOKEN_FILE = os.path.join(BASE_DIR, "credentials", "google_drive_token.json")

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
FOLDER_ID = "1IK9GbOVwgIRPJdP2hAAc9mLh2WMp64iu"


def get_drive_service():
    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_console()

        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


def find_existing_file(service, filename):
    query = (
        f"name = '{filename}' and "
        f"'{FOLDER_ID}' in parents and "
        f"trashed = false"
    )
    results = service.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name)",
        pageSize=10,
    ).execute()
    files = results.get("files", [])
    return files[0] if files else None


def upload_or_replace_file(service, filepath):
    filename = os.path.basename(filepath)
    media = MediaFileUpload(filepath, resumable=True)

    existing = find_existing_file(service, filename)

    if existing:
        updated = (
            service.files()
            .update(
                fileId=existing["id"],
                media_body=media,
                fields="id, name",
            )
            .execute()
        )
        print(f"Updated {updated['name']} -> file_id={updated['id']}")
    else:
        file_metadata = {
            "name": filename,
            "parents": [FOLDER_ID],
        }
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


def main():
    service = get_drive_service()

    target_files = [
        "system_state.json",
        "repo_map.txt",
        "context.txt",
    ]

    for fname in target_files:
        path = os.path.join(SNAPSHOT_DIR, fname)
        if os.path.exists(path):
            upload_or_replace_file(service, path)
        else:
            print(f"Missing file, skipped: {path}")


if __name__ == "__main__":
    main()
