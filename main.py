import gspread
from google.oauth2.service_account import Credentials

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

sheet_id = "1p94yHH94iSnX3j6vjY5ZIUE9hyezHgz04z8HU7QkcUM"
workbook = client.open_by_key(sheet_id)

values=workbook.sheet1.row_values(1)
print(values)