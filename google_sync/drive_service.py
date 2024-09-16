from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def get_drive_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
    return build("drive", "v3", credentials=creds)

def get_last_modified_time(fileId, drive):
    revisions = []
    pageToken = ""
    while pageToken is not None:
        res = drive.revisions().list(fileId=fileId, fields="nextPageToken,revisions(modifiedTime)", pageSize=1000, pageToken=pageToken if pageToken != "" else None).execute()
        r = res.get("revisions", [])
        revisions += r
        pageToken = res.get("nextPageToken")
    return revisions[-1]["modifiedTime"]