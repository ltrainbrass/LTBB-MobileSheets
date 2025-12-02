from __future__ import print_function
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
import re
from pprint import pprint
import os
import shutil
import sqlite3
import io
from pathlib import Path
import requests
from rich.console import Console
import builtins
import time
from dateutil import parser
from PyPDF2 import PdfReader

SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/documents.readonly"]

console = Console()
builtins.print = lambda *args, **kwargs: console.print(*args, highlight=False, **kwargs)

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

# Build Drive
creds = get_creds()
docs = build("docs", "v1", credentials=creds)
drive = build("drive", "v3", credentials=creds)

# Weekly agenda
WEEKLY_AGENDA_ID = "1jcazpKFV5wNjzDY-3oC9BKei_3iVQhrQ6xy3P76G-5w"
DEST_MUSIC_FOLDER = "1rGkyWusZDKKIk9gQAOMNpind1Oh95Zjb"
SRC_MUSIC_FOLDER = "12y2cjGE7GE3MTJ8QtNs3_Z5L30o5Ql6D"
SEASONAL_SONGS = "1M7sLr9wwvHJIfKGijRTSjC5ae1CODzbY"

# TODO
# - if no instruments:
#   - Horn means F Horn
#   - take Bb Eb F when no instruments available

# Instrument list
instruments = {
    'Score': ['Score'],
    'Tuba': ['Tuba', 'Sousaphone', 'Sousa', 'Euphonium', 'Euph', 'Low Brass', 'Basses', 'Bass (Trebel Clef)'],
    'Horn': ['Horn in F', 'F Horn', 'Mellophone', 'Horns F'],
    'Percussion': ['Percussion', 'Drum', 'Snareline', 'Perc', 'BassDr', 'Snare', 'Congo', 'Toms', 'Quads'],
    'Clarinet': ['Clarinet'],
    'Soprano Sax': ['Soprano'],
    'Tenor Sax': ['Tenor'],
    'Alto Sax': ['Alto'],
    'Bass Sax': ['Bass Sax', 'Bass Saxophone'],
    'Bari Sax': ['Bari'],
    'Trumpet': ['Trumpet', 'Flugelhorn', 'Trmp'],
    'Trombone': ['Trombone', 'Tbn', 'Trmb'],
    'Eb Horn' : ['Eb Horn', 'Horn in Eb']
}
part_folder_ids = {}
flat_instrument_list = [sub for main in instruments for sub in instruments[main]]
instrument_lookup = {}
for main in instruments:
    for sub in instruments[main]:
        instrument_lookup[sub.lower()] = main

def list_files(folder_id):
    query = f"'{folder_id}' in parents"
    result = drive.files().list(q=query, fields="files(id, name)").execute()
    return result.get("files", [])

def folder_contains_pdfs(folder_id):
    query = f"'{folder_id}' in parents and mimeType = 'application/pdf' and trashed = false"
    results = drive.files().list(
        q=query,
        fields="files(id, name)",
        pageSize=1  # we only need to know if at least one exists
    ).execute()
    
    return len(results.get("files", [])) > 0

# Figure out instrumentation from song titles and which files belong to which instrument
def assemble_song_parts(song):
    files = song['files']
    song['parts'] = {}
    for file in files:
        file_name = file['name']
        instruments = extract_instruments(file_name)
        for instrument in instruments:
            if instrument not in song['parts']:
                song['parts'][instrument] = []
            song['parts'][instrument].append(file)
            print("    [magenta]" + instrument + "[/magenta]: [green]" + file['name'])

no_instrument_files = []

# Function for getting a sanitized instrument name
def extract_instruments(file_name):
    if not file_name.endswith('.pdf'):
        return []
    instruments = []
    for possible_instrument in instrument_lookup:
        if possible_instrument.replace(' ', '_') in file_name.lower().replace(' ', '_').replace('.',''):
            instruments.append(instrument_lookup[possible_instrument])
    if not instruments:
        print("    [yellow]INSTRUMENT NOT FOUND: " + file_name)
        no_instrument_files.append(file_name)
    return instruments

def get_folder_name(folder_id):
    return drive.files().get(
        fileId=folder_id,
        fields="id, name"
    ).execute()['name']

def assemble_song_from_folder(folder_id):
    if not folder_contains_pdfs(folder_id):
        return None
    song = {}
    song['title'] = get_folder_name(folder_id)
    song['files'] = list_files(folder_id)

    assemble_song_parts(song)
    return song

# Scrapes a doc and extracts all songs linked
def scrape_song_list(doc_id):
    print()
    print('[cyan]Assembling song metadata from Doc')
    doc = docs.documents().get(documentId=doc_id).execute()
    content = doc["body"]["content"]

    links = []

    # Find links
    for element in content:
        if "paragraph" not in element:
            continue
        for run in element["paragraph"]["elements"]:
            text = run.get("textRun", {})
            if "textStyle" in text and "link" in text["textStyle"]:
                links.append(text["textStyle"]["link"]["url"])

    links = [link for link in links if bool(re.match(r"^https://drive\.google\.com/drive/.*folders/.*", link))]
    links = links[:2]

    songs = []
    # Get files at Drive links
    for link in links:
        folder_id = extract_folder_id(song["link"])
        song = assemble_song_from_folder(folder_id)
        songs.append(song)

    if len(no_instrument_files) > 0:
        no_instrument_files.sort()
        print('[yellow] Some files had no instruments:')
        for file in no_instrument_files:
            print(    '[yellow]' + file)
    return songs

def list_subfolders(parent_folder_id):
    query = (
        f"'{parent_folder_id}' in parents "
        "and mimeType = 'application/vnd.google-apps.folder' "
        "and trashed = false"
    )
    results = drive.files().list(
        q=query,
        fields="files(id, name)",
        pageSize=1000  # adjust if needed
    ).execute()
    
    return results.get("files", [])

IGNORE_FOLDERS = ['1. Member Drafts', '2. Seasonal Songs', '3. Warm-ups', '4. 3rd Rail Drumline', '5. Resources', '6. Recordings']
MAX_SONGS = 999999999

def scrape_folder_of_songs(folder_ids):
    print()
    print('[cyan]Assembling song metadata from folders')
    songs = []
    i = 0
    for folder_id in folder_ids:
        for cat_folder in list_subfolders(folder_id):
            if cat_folder['name'] in IGNORE_FOLDERS:
                continue
            if i > MAX_SONGS:
                break
            for song_folder in list_subfolders(cat_folder['id']):
                if i > MAX_SONGS:
                    break
                print("Assembling song metadata [bold green]'" + song_folder['name'] + "'[/bold green] with ID '" + song_folder['id'] + "'")
                song = assemble_song_from_folder(song_folder['id'])
                if song:
                    songs.append(song)
                    i += 1
    if len(no_instrument_files) > 0:
        print('[yellow] Some files had no instruments:')
        for file in no_instrument_files:
            print(    '[yellow]' + file)
    return songs

# Drive Create folder
def create_folder(name, parent_id=None):
    file_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder"
    }
    if parent_id:
        file_metadata["parents"] = [parent_id]

    folder = drive.files().create(
        body=file_metadata,
        fields="id, name, parents"
    ).execute()
    print("Created folder ", name, ":", folder['id'])

    return folder

# Drive Create folder if does not exist
def get_or_create_folder(name, parent_id=None):
    query = f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder'"
    if parent_id:
        query += f" and '{parent_id}' in parents"

    results = drive.files().list(
        q=query,
        fields="files(id, name)",
        spaces="drive"
    ).execute()

    files = results.get("files", [])
    if files:
        part_folder_ids[name] = files[0]['id']
        return files[0]   # already exists

    # otherwise create it
    file = create_folder(name=name, parent_id=parent_id)
    part_folder_ids[name] = file['id']
    return file


def escape_drive_query(name):
    # escape single quotes by doubling them
    return name.replace("'", "\\'")

# Drive copy file if newer
def sync_file(source_file_id, dest_folder_id, new_name=None):
    # Get source file metadata
    source = drive.files().get(
        fileId=source_file_id,
        fields="id, name, modifiedTime"
    ).execute()
    source_name = new_name or source["name"]
    source_modified = source["modifiedTime"]

    # Check if a file with the same name exists in destination
    query = f"name contains '{escape_drive_query(source_name)}' and '{dest_folder_id}' in parents and trashed = false"
    results = drive.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
    existing_files = results.get("files", [])

    if existing_files:
        existing = existing_files[0]
        existing_modified = existing["modifiedTime"]

        # Compare modified timestamps
        if source_modified > existing_modified:
            print(f"        Source file is newer. Replacing '{source_name}'")
            # Delete the old copy
            drive.files().delete(fileId=existing["id"]).execute()
        else:
            print(f"        Existing file '{source_name}' is up-to-date. Skipping copy.")
            return existing["id"]  # nothing to do

    # Copy the source file into the folder
    new_file_metadata = {"parents": [dest_folder_id], "name": source_name}
    copied_file = drive.files().copy(fileId=source_file_id, body=new_file_metadata, fields="id, name").execute()
    print(f"        Copied [green]'{source_name}'[/green] to folder")
    return copied_file["id"]

def get_file_metadata(file_id):
    return drive.files().get(
        fileId=file_id,
        fields="id, name, mimeType, size, createdTime, modifiedTime, md5Checksum, parents"
    ).execute()

def copy_songlist_into_drive(songs):
    print()
    print('[cyan]Copying songs into Google Drive folders')
    # Make copies of files to my Drive
    for song in songs:
        if song is None:
            print('[red]ERROR - no song!')
        print("Copying files for [green]" + song['title'])
        for part_key in song['parts']:
            part_charts = song['parts'][part_key]
            print("    Instrument [magenta]" + part_key)
            # Some parts have more than one chart (trumpet 1/2)
            for part_chart in part_charts:
                part_folder = get_or_create_folder(part_key, DEST_MUSIC_FOLDER)
                copied = sync_file(
                    source_file_id=part_chart['id'],
                    dest_folder_id=part_folder['id'],
                    new_name=part_chart['name']
                )

            # print("New file ID:", copied)

def upload_to_drive(local_path, dest_name, parent_folder_id):
    # Look for existing file with this exact name in this exact folder
    query = (
        f"name = '{dest_name}' "
        f"and '{parent_folder_id}' in parents "
        "and trashed = false"
    )

    results = drive.files().list(
        q=query,
        fields="files(id, name)"
    ).execute()

    # Delete existing file(s) with that name
    for f in results.get("files", []):
        print(f"Deleting old {f['name']} ({f['id']})")
        drive.files().delete(fileId=f["id"]).execute()

    # Upload the new file
    file_metadata = {
        "name": dest_name,
        "parents": [parent_folder_id],
    }

    media = MediaFileUpload(local_path, resumable=True)

    uploaded = drive.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, name"
    ).execute()

    print(f"Uploaded {uploaded['name']} ({uploaded['id']})")
    return uploaded["id"]

def clear_ouptput_folder():
    folder = 'output'
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if os.path.isfile(path):
            os.remove(path)
            print(f"    [cyan]Deleted {path}")

def create_database(db_name):
    db_path = 'output/' + db_name.replace(' ','_').lower() + '.db'
    if os.path.exists(db_path):
        print('    Removing old ' + db_path + ' and replacing with a blank fresh library db')
        os.remove(db_path)
    print('    Created ' + db_path)
    shutil.copy("ltbb_blank.db", db_path)

def java_string_hashcode(s: str) -> int:
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h & 0x80000000:
        h = -((~h + 1) & 0xFFFFFFFF)
    return h

def get_page_count(path):
    reader = PdfReader(path)
    return len(reader.pages)

def update_database(songs):
    print()
    print("[cyan]Updating databases")

    # Create database files
    clear_ouptput_folder()
    used_instruments = set()
    for song in songs:
        for part in song['parts']:
            if part not in used_instruments:
                used_instruments.add(part)
    for instrument in used_instruments:
        create_database(instrument)

    part_song_ids = {}
    for part in used_instruments:
        part_song_ids[part] = 0

    for song in songs:
        print("Gathering metadata for [green]" + song['title'])
        for file in song['files']:
            print('    Downloading file [green]' + file['name'])
            metadata = get_file_metadata(file['id'])
            if metadata['mimeType'] != 'application/pdf':
                continue
            file['filesize'] = int(metadata['size'])
            file['filehash'] = java_string_hashcode(file['name'])
            # if file['name'] == 'Wipe_Eauxt_1_2 - Tenor Sax.pdf':
            #     print("    [red]Subbing hash code")
            #     file['filehash'] = 142563180
            file['filesize'] = int(file.get("filesize", 0))
            dt = parser.isoparse(metadata['modifiedTime'])
            file['lastmodified'] = int(dt.timestamp() * 1000)
            dt = parser.isoparse(metadata['createdTime'])
            file['creationdate'] = int(dt.timestamp() * 1000)

            # Download the file
            request = drive.files().get_media(fileId=file['id'])
            fh = io.FileIO('intermediate/temp_file.pdf', 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            file['pagecount'] = get_page_count('intermediate/temp_file.pdf')
            file['pageorder'] = '1-' + str(file['pagecount'])

        for part in song['parts']:
            db_path = 'output/' + part.replace(' ','_').lower() + '.db'
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()

            for file in song['parts'][part]:
                part_song_ids[part] += 1
                song_id = part_song_ids[part]
                print("Inserting Song [green]" + file['name'])
                
                cur.execute("""
                INSERT INTO Songs (Title, Difficulty, LastPage, OrientationLock, Duration, Stars, VerticalZoom, Sharpen, SharpenLevel, CreationDate, LastModified, Keywords, AutoStartAudio, SongId)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (file['name'][:-4], 0, 0, 0, 0, 0, 1.0, 0, 7, file['creationdate'], file['lastmodified'], "", 0, part_song_ids[part]))

                cur.execute("""
                INSERT INTO Files (SongId, Path, PageOrder, FileSize, LastModified, Source, Type, SourceFilePageCount, FileHash, Width, Height)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (song_id, part_folder_ids[part] + '/' + file['name'], file['pageorder'], file['filesize'], file['lastmodified'], 1, 1, file['pagecount'], file['filehash'], -1, -1))

                cur.execute("""
                INSERT INTO AutoScroll (SongId, Behavior, PauseDuration, Speed, FixedDuration, ScrollPercent, ScrollOnLoad, TimeBeforeScroll)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (song_id, 0, 8000, 3, 1000, 20, 0, 2000))

                for i in range(file['pagecount']):
                    cur.execute("""
                    INSERT INTO Crop (SongId, Page, Left, Top, Right, Bottom, Rotation)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (song_id, i, 0, 0, 0, 0, 0))

                for i in range(file['pagecount']):
                    cur.execute("""
                    INSERT INTO ZoomPerPage (SongId, Page, Zoom, PortPanX, PortPanY, LandZoom, LandPanX, LandPanY, FirstHalfY, SecondHalfY)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (song_id, i, 100.0, 0, 0, 100.0, 0, 0, 0, 0))

                cur.execute("""
                INSERT INTO MetronomeSettings (SongId, Sig1, Sig2, Subdivision, SoundFX, AccentFirst, AutoStart, CountIn, NumberCount, AutoTurn)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (song_id, 2, 0, 0, 0, 0, 0, 0, 1, 0))

                for i in range(file['pagecount']):
                    cur.execute("""
                    INSERT INTO MetronomeBeatsPerPage (SongId, Page, BeatsPerPage)
                    VALUES (?, ?, ?)""",
                    (song_id, i, 0))

                # cur.execute("""
                # INSERT INTO ZoomPerPage ()
                # VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                # (1, 0, 8000, 3, 1000, 20, 0, 2000))

                # Add line to hashcodes
                with open('output/'+part.replace(' ','_').lower() + '_hashcodes.txt', "a", encoding="utf-8") as f_out:
                    f_out.write(f"{part_folder_ids[part]}/{file['name']}\n")
                    f_out.write(f"{file['filehash']}\n")
                    f_out.write(f"{file['lastmodified']}\n")
                    f_out.write(f"{file['filesize']}\n")

            conn.commit()
            conn.close()

    for instrument in used_instruments:
        db_name = instrument.replace(' ','_').lower() + '.db'
        hashcodes_name = instrument.replace(' ','_').lower() + '_hashcodes.txt'
        print('Uploading [cyan]output/' + db_name + '[/cyan] and [cyan]' + hashcodes_name + '[/cyan] to [green]' + instrument)
        part_folder = part_folder_ids[instrument]
        upload_to_drive(local_path='output/'+db_name, dest_name='mobilesheets.db', parent_folder_id = part_folder)
        upload_to_drive(local_path='output/'+hashcodes_name, dest_name='mobilesheets_hashcodes.txt', parent_folder_id = part_folder)

# print(java_string_hashcode('1tdkwYTPnSlXTZeZwPyLTAhCb73l0ngse//Wipe_Eauxt_1_2 - Tenor Sax.pdf'))
# exit()

# Assemble song list
songs = scrape_folder_of_songs([SRC_MUSIC_FOLDER, SEASONAL_SONGS])
print()
print("Songs:")
pprint(songs)
copy_songlist_into_drive(songs)
update_database(songs)


# drive = build("drive", "v3", credentials=creds)
