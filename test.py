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

    cursor.execute(f"SHOW COLUMNS FROM {sheet_name}")
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
        alter_query = f"ALTER TABLE {sheet_name} ADD COLUMN {col} VARCHAR(255)"
        cursor.execute(alter_query)

    # Detect removed columns in Google Sheets and drop them from MySQL
    removed_columns = [col for col in mysql_columns if col not in google_columns and col != 'id']
    for col in removed_columns:
        alter_query = f"ALTER TABLE {sheet_name} DROP COLUMN {col}"
        cursor.execute(alter_query)

    conn.commit()
    cursor.close()
    conn.close()

# Create MySQL table if it doesn't exist
def create_mysql_table(sheet_name, columns):
    conn = connect_mysql()
    cursor = conn.cursor()

    # Add a primary key column for efficient updates
    create_table_query = f"CREATE TABLE IF NOT EXISTS {sheet_name} (id INT PRIMARY KEY AUTO_INCREMENT, "
    create_table_query += ", ".join([f"{col} VARCHAR(255)" for col in columns])
    create_table_query += ");"

    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# Insert or update rows in MySQL
def sync_sheet_to_db(sheet_name, google_sheet_data):
    conn = connect_mysql()
    cursor = conn.cursor()

    # Fetch existing data from MySQL
    cursor.execute(f"SELECT * FROM {sheet_name}")
    mysql_data = cursor.fetchall()

    # Convert MySQL data to dictionary (excluding 'id' column)
    mysql_dict = {tuple(row[1:]): row[0] for row in mysql_data}  # {row_data: id}

    # Track Google Sheets data that is new or needs updating
    google_data_dict = {tuple(row): idx + 1 for idx, row in enumerate(google_sheet_data[1:])}  # Ignore header

    # Insert or update rows
    for row in google_data_dict:
        if row not in mysql_dict:
            # Row is new, insert it
            placeholders = ", ".join(["%s"] * len(row))
            column_list = ", ".join(google_sheet_data[0])
            insert_query = f"INSERT INTO {sheet_name} ({column_list}) VALUES ({placeholders})"
            cursor.execute(insert_query, row)

    # Delete rows from MySQL that are no longer in Google Sheets
    for row in mysql_dict:
        if row not in google_data_dict:
            delete_query = f"DELETE FROM {sheet_name} WHERE id = {mysql_dict[row]}"
            cursor.execute(delete_query)

    conn.commit()
    cursor.close()
    conn.close()

# Sync Google Sheets to MySQL and adjust schema if needed
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
