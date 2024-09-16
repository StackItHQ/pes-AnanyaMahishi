import time
from google_sync.google_sheets import connect_google_sheets, fetch_google_sheet_data
from google_sync.drive_service import get_drive_service, get_last_modified_time
from database.sheet_to_mysql import sync_google_sheets_to_mysql
from database.mysql_to_sheet import sync_db_to_google_sheets, fetch_mysql_data
from utils.checksum import compute_checksum

if __name__ == "__main__":
    sheet_id = "1p94yHH94iSnX3j6vjY5ZIUE9hyezHgz04z8HU7QkcUM"
    sheet_name = "sheet1"  # Same name as MySQL table

    # Connect to Google Sheets and Drive
    sheet = connect_google_sheets(sheet_id)
    drive_service = get_drive_service()
    
    last_modified_time = None
    last_mysql_data_checksum = None
    google_sheet_data = None

    while True:
        current_modified_time = get_last_modified_time(sheet_id, drive_service)
        
        if last_modified_time != current_modified_time:
            google_sheet_data = fetch_google_sheet_data(sheet)
            print("Changes detected in Google Sheets. Syncing Google Sheets to MySQL...")
            sync_google_sheets_to_mysql(sheet, sheet_name, google_sheet_data)
            last_modified_time = current_modified_time
        
        mysql_data = fetch_mysql_data(sheet_name)
        current_mysql_data_checksum = compute_checksum(mysql_data)
        
        if last_mysql_data_checksum != current_mysql_data_checksum:
            print("Changes detected in MySQL data. Updating Google Sheets...")
            sync_db_to_google_sheets(sheet, sheet_name, google_sheet_data)
            last_mysql_data_checksum = current_mysql_data_checksum

        time.sleep(10)
