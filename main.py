from __future__ import print_function
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import re

SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/documents.readonly"]

def get_creds():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds

def extract_folder_id(url):
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    return match.group(1) if match else None

def list_files(folder_id):
    query = f"'{folder_id}' in parents"
    result = drive.files().list(q=query, fields="files(id, name)").execute()
    return result.get("files", [])

creds = get_creds()
docs = build("docs", "v1", credentials=creds)

DOC_ID = "1jcazpKFV5wNjzDY-3oC9BKei_3iVQhrQ6xy3P76G-5w"

doc = docs.documents().get(documentId=DOC_ID).execute()
content = doc["body"]["content"]

links = []

for element in content:
    if "paragraph" not in element:
        continue
    for run in element["paragraph"]["elements"]:
        text = run.get("textRun", {})
        if "textStyle" in text and "link" in text["textStyle"]:
            link = text["textStyle"]["link"]["url"]
            links.append((text["content"], link))

for link in links:
    print(link)

# drive = build("drive", "v3", credentials=creds)
