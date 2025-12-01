from __future__ import print_function
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import re
from pprint import pprint
import os
import sqlite3

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

creds = get_creds()
docs = build("docs", "v1", credentials=creds)
drive = build("drive", "v3", credentials=creds)

def list_files(folder_id):
    query = f"'{folder_id}' in parents"
    result = drive.files().list(q=query, fields="files(id, name)").execute()
    return result.get("files", [])

# Weekly agenda
DOC_ID = "1jcazpKFV5wNjzDY-3oC9BKei_3iVQhrQ6xy3P76G-5w"

doc = docs.documents().get(documentId=DOC_ID).execute()
content = doc["body"]["content"]

songs = []

# Find links
for element in content:
    if "paragraph" not in element:
        continue
    for run in element["paragraph"]["elements"]:
        text = run.get("textRun", {})
        if "textStyle" in text and "link" in text["textStyle"]:
            songs.append({"title": text["content"], "link": text["textStyle"]["link"]["url"]})

songs = [song for song in songs if  bool(re.match(r"^https://drive\.google\.com/drive/.*folders/.*", song["link"]))]
songs = songs[:5]

# Get files at Drive links
for song in songs:
    print(song["title"], ":", extract_folder_id(song["link"]))
    folder_id = extract_folder_id(song["link"])
    song['files'] = list_files(folder_id)

# Instrument list
instruments = {
    'Score': ['Score'],
    'Tuba': ['Tuba', 'Sousaphone', 'Euphonium', 'Euph T.C.', 'Euph (T.C.)', 'Euph TC'],
    'Horn': ['Horn in F', 'F Horn', 'Mellophone'],
    'Percussion': ['Percussion', 'Drum', 'Snareline', 'Perc 1', 'Perc 2'],
    'Clarinet': ['Clarinet'],
    'Tenor Sax': ['Tenor'],
    'Alto Sax': ['Alto'],
    'Bass Sax': ['Bass Sax', 'Bass Saxophone'],
    'Bari Sax': ['Bari'],
    'Trumpet': ['Trumpet', 'Flugelhorn'],
    'Trombone': ['Trombone']
}
instruments = {main.lower() : [sub.lower() for sub in instruments[main]] for main in instruments}
flat_instrument_list = [sub for main in instruments for sub in instruments[main]]
instrument_lookup = {}
for main in instruments:
    for sub in instruments[main]:
        instrument_lookup[sub] = main

# Function for getting a sanitized instrument name
def extract_instrument(file_name):
    if not file_name.endswith('.pdf'):
        return None
    file_name = file_name[:-4]
    if "-" not in file_name:
        print("NO DASH - skipping", file_name)
        return None
    file_split = file_name.split('-')
    if len(file_split) != 2:
        print("Split on dash didn't work, skipping", file_name)
        return None
    instrument = None
    unsanitized_instruments = []
    unsanitized_instruments.append(file_split[0].lower().strip())
    unsanitized_instruments.append(file_split[1].lower().strip())
    for unsanitized_instrument in unsanitized_instruments:
        for possible_instrument in instrument_lookup:
            if unsanitized_instrument in possible_instrument or possible_instrument in unsanitized_instrument:
                instrument = instrument_lookup[possible_instrument]
                break
        if instrument != None:
            break
    if instrument == None:
        print("INSTRUMENT NOT FOUND: ", file_name)
        return None
    return instrument

# Figure out instrumentation and which files belong to which instrument
for song in songs:
    files = song['files']
    song['parts'] = {}
    for file in files:
        file_name = file['name']
        instrument = extract_instrument(file_name)
        if instrument is None:
            continue
        if instrument not in song['parts']:
            song['parts'][instrument] = []
        song['parts'][instrument].append(file)

print()
print("Song 1:")
pprint(songs[1])

# Database time

db_path = "songs.db"

if os.path.exists(db_path):
    os.remove(db_path)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

cur.execute("CREATE TABLE Songs(Id INTEGER PRIMARY KEY,Title VARCHAR(255),Difficulty INTEGER,Custom VARCHAR(255) DEFAULT '',Custom2 VARCHAR(255) DEFAULT '',LastPage INTEGER,OrientationLock INTEGER,Duration INTEGER,Stars INTEGER DEFAULT 0,VerticalZoom FLOAT DEFAULT 1,SortTitle VARCHAR(255) DEFAULT '',Sharpen INTEGER DEFAULT 0,SharpenLevel INTEGER DEFAULT 4,CreationDate INTEGER DEFAULT 0,LastModified INTEGER DEFAULT 0,Keywords VARCHAR(255) DEFAULT '',AutoStartAudio INTEGER,SongId INTEGER)")
cur.execute("CREATE TABLE Files(Id INTEGER PRIMARY KEY,SongId INTEGER,Path VARCHAR(255),PageOrder VARCHAR(255),FileSize INTEGER,LastModified INTEGER,Source INTEGER,Type INTEGER,Password VARCHAR(255) DEFAULT '',SourceFilePageCount INTEGER,FileHash INTEGER,Width INTEGER,Height INTEGER)")
cur.execute("CREATE INDEX files_song_id_idx ON Files(SongId)")

cur.execute("""
INSERT INTO Songs (Title, Difficulty, LastPage, OrientationLock, Duration, Stars, VerticalZoom, Sharpen, SharpenLevel, CreationDate, LastModified, Keywords, AutoStartAudio, SongId)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
("Test Song", 0, 0, 0, 0, 0, 0, 1.0, 0, 7, 1234567, 1234567, "", 0, 0))

conn.commit()
conn.close()

# drive = build("drive", "v3", credentials=creds)
