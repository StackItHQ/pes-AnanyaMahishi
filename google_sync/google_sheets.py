import gspread
from google.oauth2.service_account import Credentials

def connect_google_sheets(sheet_id):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
    client = gspread.authorize(creds)
    workbook = client.open_by_key(sheet_id)
    return workbook.sheet1

def fetch_google_sheet_data(sheet):
    return sheet.get_all_values()
