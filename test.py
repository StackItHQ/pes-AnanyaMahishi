import gspread
from google.oauth2.service_account import Credentials
import mysql.connector
import time

# Google Sheets authentication
def connect_google_sheets(sheet_id):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
    client = gspread.authorize(creds)
    workbook = client.open_by_key(sheet_id)
    return workbook

# Fetch data from Google Sheets
def fetch_google_sheet_data(sheet):
    return sheet.get_all_values()  # Fetches all data as 2D list

# MySQL connection
def connect_mysql():
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',  # No password
        database='supsql'  # Ensure the database exists
    )
    return connection

# Fetch existing MySQL columns for a table
def get_mysql_columns(sheet_name):
    conn = connect_mysql()
    cursor = conn.cursor()

    cursor.execute(f"SHOW COLUMNS FROM `{sheet_name}`")
    columns = [col[0] for col in cursor.fetchall()]

    cursor.close()
    conn.close()
    return columns

# Sync the structure of the MySQL table to match Google Sheets
def sync_table_structure(sheet_name, google_columns):
    conn = connect_mysql()
    cursor = conn.cursor()

    # Fetch existing columns in MySQL
    mysql_columns = get_mysql_columns(sheet_name)

    # Detect new columns in Google Sheets and add them to MySQL
    new_columns = [col for col in google_columns if col not in mysql_columns]
    for col in new_columns:
        alter_query = f"ALTER TABLE `{sheet_name}` ADD COLUMN `{col}` VARCHAR(255)"
        cursor.execute(alter_query)

    # Detect removed columns in Google Sheets and drop them from MySQL
    removed_columns = [col for col in mysql_columns if col not in google_columns and col != 'row_number']
    for col in removed_columns:
        alter_query = f"ALTER TABLE `{sheet_name}` DROP COLUMN `{col}`"
        cursor.execute(alter_query)

    conn.commit()
    cursor.close()
    conn.close()

# Create MySQL table if it doesn't exist, using row_number as the primary key
def create_mysql_table(sheet_name, columns):
    conn = connect_mysql()
    cursor = conn.cursor()

    # Add a 'row_number' as the primary key, and escape column names
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{sheet_name}` (`row_number` INT PRIMARY KEY, "
    create_table_query += ", ".join([f"`{col}` VARCHAR(255)" for col in columns])
    create_table_query += ");"

    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# Insert or update rows in MySQL using row_number as the key
def sync_sheet_to_db(sheet_name, google_sheet_data):
    conn = connect_mysql()
    cursor = conn.cursor()

    # Prepare column names and data
    columns = google_sheet_data[0]  # The header row
    data_rows = google_sheet_data[1:]  # The actual data

    for row_number, row in enumerate(data_rows, start=1):
        # Prepare query for inserting or updating data
        placeholders = ", ".join(["%s"] * len(row))
        column_list = ", ".join([f"`{col}`" for col in columns])
        update_query = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in columns])

        insert_query = f"""
        INSERT INTO `{sheet_name}` (`row_number`, {column_list}) VALUES ({row_number}, {placeholders})
        ON DUPLICATE KEY UPDATE {update_query};
        """
        cursor.execute(insert_query, row)

    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

# Continuously sync Google Sheets to MySQL and adjust schema if needed
def sync_google_sheets_to_mysql(sheet, sheet_name):
    # Fetch Google Sheets data
    google_sheet_data = fetch_google_sheet_data(sheet)

    # Create MySQL table if it doesn't exist
    create_mysql_table(sheet_name, google_sheet_data[0])

    # Sync table structure (add/remove columns)
    sync_table_structure(sheet_name, google_sheet_data[0])

    # Sync Google Sheets data to MySQL
    sync_sheet_to_db(sheet_name, google_sheet_data)

    print(f"Synced Google Sheet data to MySQL at {time.ctime()}")

    time.sleep(10)  # Poll Google Sheets for changes every 10 seconds

if __name__ == "__main__":
    sheet_id = "1p94yHH94iSnX3j6vjY5ZIUE9hyezHgz04z8HU7QkcUM"
    sheet = connect_google_sheets(sheet_id).sheet1  # First sheet in the workbook
    sheet_name = "sheet1"  # Same name as MySQL table

    while True:
        sync_google_sheets_to_mysql(sheet, sheet_name)
