"""
Downloads new vendor quote files from Google Drive.
Tracks previously processed files via a lookback window.
Supports Shared Drives (supportsAllDrives).
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io


SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


def get_drive_service():
    """Authenticate with Google Drive using service account."""
    creds_json = os.environ['GOOGLE_SERVICE_ACCOUNT_KEY']
    creds_dict = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES
    )
    return build('drive', 'v3', credentials=credentials)


def find_vendor_folder(service, root_folder_id, vendor_name):
    """Find the vendor-specific subfolder in the Drive root."""
    query = (
        f"'{root_folder_id}' in parents "
        f"and name = '{vendor_name}' "
        f"and mimeType = 'application/vnd.google-apps.folder' "
        f"and trashed = false"
    )
    results = service.files().list(
        q=query,
        fields='files(id, name)',
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = results.get('files', [])
    if not files:
        # Debug: list what the service account can see
        debug_query = f"'{root_folder_id}' in parents and trashed = false"
        debug_results = service.files().list(
            q=debug_query,
            fields='files(id, name, mimeType)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        debug_files = debug_results.get('files', [])
        print(f"DEBUG: Root folder contains {len(debug_files)} items:")
        for f in debug_files:
            print(f"  - {f['name']} ({f['mimeType']}) id={f['id']}")
        raise FileNotFoundError(f"Vendor folder '{vendor_name}' not found in Drive")
    return files[0]['id']


def list_new_files(service, folder_id, lookback_hours=26):
    """List files created in the last N hours (slightly over 24h for overlap safety)."""
    cutoff = (datetime.utcnow() - timedelta(hours=lookback_hours)).isoformat() + 'Z'
    query = (
        f"'{folder_id}' in parents "
        f"and createdTime > '{cutoff}' "
        f"and trashed = false"
    )
    results = service.files().list(
        q=query,
        fields='files(id, name, mimeType, size, createdTime)',
        orderBy='createdTime desc',
        pageSize=100,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    return results.get('files', [])


def download_file(service, file_id, file_name, output_dir):
    """Download a single file from Drive."""
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, file_name)

    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    with io.FileIO(filepath, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    return filepath


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--vendor', required=True)
    parser.add_argument('--output-dir', required=True)
    args = parser.parse_args()

    root_folder_id = os.environ['DRIVE_ROOT_FOLDER_ID']
    service = get_drive_service()

    vendor_folder_id = find_vendor_folder(service, root_folder_id, args.vendor)
    new_files = list_new_files(service, vendor_folder_id)

    print(f"Found {len(new_files)} new files for {args.vendor}")

    # Set output for GitHub Actions
    github_output = os.environ.get('GITHUB_OUTPUT')
    if github_output:
        with open(github_output, 'a') as f:
            f.write(f"file_count={len(new_files)}\n")

    for file_info in new_files:
        path = download_file(
            service, file_info['id'], file_info['name'], args.output_dir
        )
        print(f"  Downloaded: {path} ({file_info.get('size', '?')} bytes)")

    # Write manifest for downstream steps
    os.makedirs(args.output_dir, exist_ok=True)
    manifest_path = os.path.join(args.output_dir, '_manifest.json')
    with open(manifest_path, 'w') as f:
        json.dump({
            'vendor': args.vendor,
            'download_time': datetime.utcnow().isoformat(),
            'files': new_files
        }, f, indent=2)


if __name__ == '__main__':
    main()
