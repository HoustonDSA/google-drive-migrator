import argparse
import os
import pickle
import sys
import time

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

FOLDER_MIME = 'application/vnd.google-apps.folder'
SCOPES = ['https://www.googleapis.com/auth/drive']


def authenticate_oauth(client_secrets_file='client_secrets.json', token_file='token.pickle'):
    creds = None
    if os.path.exists(token_file):
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)


def list_folder_contents(service, folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    return results.get('files', [])


def create_folder(service, name, parent_id):
    metadata = {
        'name': name,
        'mimeType': FOLDER_MIME,
        'parents': [parent_id]
    }
    new_folder = service.files().create(
        body=metadata,
        fields='id',
        supportsAllDrives=True
    ).execute()
    return new_folder['id']


def copy_file(service, file_id, name, parent_id):
    metadata = {
        'name': name,
        'parents': [parent_id]
    }
    copied = service.files().copy(
        fileId=file_id,
        body=metadata,
        fields='id',
        supportsAllDrives=True
    ).execute()
    return copied['id']


def get_permissions(service, file_id):
    permissions = service.permissions().list(
        fileId=file_id,
        fields="permissions(id, type, role, emailAddress, domain)",
        supportsAllDrives=True
    ).execute().get('permissions', [])

    return [
        p for p in permissions
        if p['role'] != 'owner' and p['type'] in ('user', 'group', 'domain')
    ]


def copy_permissions(service, src_id, dst_id):
    permissions = get_permissions(service, src_id)
    for perm in permissions:
        try:
            body = {
                'type': perm['type'],
                'role': perm['role'],
            }
            if perm['type'] in ('user', 'group'):
                body['emailAddress'] = perm['emailAddress']
            elif perm['type'] == 'domain':
                body['domain'] = perm['domain']

            service.permissions().create(
                fileId=dst_id,
                body=body,
                fields='id',
                sendNotificationEmail=False,
                supportsAllDrives=True
            ).execute()
            print(f"    🔐 Copied permission for {perm.get('emailAddress', perm.get('domain'))}")
        except HttpError as e:
            print(f"    ⚠️ Failed to copy permission: {e}")


def recursive_copy(service, src_folder_id, dst_folder_id, indent=0):
    items = list_folder_contents(service, src_folder_id)

    for item in items:
        name = item['name']
        mime = item['mimeType']
        prefix = ' ' * indent

        if mime == FOLDER_MIME:
            print(f"{prefix}📁 Folder: {name}")
            try:
                new_folder_id = create_folder(service, name, dst_folder_id)
                copy_permissions(service, item['id'], new_folder_id)
                recursive_copy(service, item['id'], new_folder_id, indent + 2)
            except HttpError as e:
                print(f"{prefix}✗ Error copying folder '{name}': {e}")
        else:
            print(f"{prefix}📄 File: {name}")
            try:
                new_file_id = copy_file(service, item['id'], name, dst_folder_id)
                copy_permissions(service, item['id'], new_file_id)
                time.sleep(0.1)
            except HttpError as e:
                print(f"{prefix}✗ Error copying file '{name}': {e}")


def main():
    parser = argparse.ArgumentParser(description='Copy a Google Drive folder to a Shared Drive with OAuth.')
    parser.add_argument('--client-secrets', default='client_secrets.json', help='OAuth 2.0 client secrets file')
    parser.add_argument('--src', required=True, help='Source folder ID')
    parser.add_argument('--dst', required=True, help='Destination folder ID')
    args = parser.parse_args()

    try:
        service = authenticate_oauth(args.client_secrets)
        print(f"\n🔄 Starting copy:\n  From: {args.src}\n  To:   {args.dst}\n")
        recursive_copy(service, args.src, args.dst)
        print("\n✅ Done copying folder with permissions!")
    except Exception as e:
        print(f"\n✗ Fatal error: {e}\n")
        sys.exit(1)


if __name__ == '__main__':
    main()
