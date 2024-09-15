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
    return workbook.sheet1  # Return the first sheet in the workbook

# Fetch data from Google Sheets
def fetch_google_sheet_data(sheet):
    return sheet.get_all_values()  # Fetches all data as a 2D list

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

    # Remove 'row_number' if it is in the list of columns
    columns = [col for col in columns if col != 'row_number']

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

# Fetch data from MySQL
def fetch_mysql_data(sheet_name):
    conn = connect_mysql()
    cursor = conn.cursor()

    # Fetch column names and exclude 'row_number'
    cursor.execute(f"SHOW COLUMNS FROM `{sheet_name}`")
    columns = [col[0] for col in cursor.fetchall() if col[0] != 'row_number']
    
    # Construct the query to select all columns except 'row_number'
    column_list = ", ".join(f"`{col}`" for col in columns)
    query = f"SELECT {column_list} FROM `{sheet_name}`"

    cursor.execute(query)
    data = cursor.fetchall()
    
    # Add the columns as the first row of data
    data_with_columns = [columns] + data

    cursor.close()
    conn.close()
    
    return data_with_columns

# Efficiently update Google Sheets with only changed data
def update_google_sheets(sheet, new_data):
    existing_data = fetch_google_sheet_data(sheet)
    
    # Assuming first row is header
    existing_headers = existing_data[0]
    new_headers = new_data[0]
    
    # Update headers if needed
    if existing_headers != new_headers:
        sheet.resize(len(new_data) + 1, len(new_headers))
        sheet.update('A1', [new_headers])
    
    # Start updating from row 2
    start_row = 2
    for row_number, new_row in enumerate(new_data[1:], start=start_row):
        existing_row = existing_data[row_number - start_row + 1] if row_number - start_row + 1 < len(existing_data) else None
        
        # Compare and update only changed cells
        if existing_row:
            for col_number, (existing_cell, new_cell) in enumerate(zip(existing_row, new_row)):
                if existing_cell != new_cell:
                    cell_address = gspread.utils.rowcol_to_a1(row_number, col_number + 1)
                    sheet.update(cell_address, [[new_cell]])
        else:
            # If row is new, update the entire row
            cell_range = f'A{row_number}:{gspread.utils.rowcol_to_a1(row_number, len(new_row))}'
            sheet.update(cell_range, [new_row])


# Sync MySQL data to Google Sheets
def sync_db_to_google_sheets(sheet, sheet_name):
    # Fetch MySQL data
    mysql_data = fetch_mysql_data(sheet_name)

    # Update Google Sheets
    update_google_sheets(sheet, mysql_data)

    print(f"Synced MySQL data to Google Sheets at {time.ctime()}")

# Continuously sync Google Sheets to MySQL and adjust schema if needed
def sync_google_sheets_to_mysql(sheet, sheet_name):
    # Fetch Google Sheets data
    google_sheet_data = fetch_google_sheet_data(sheet)

    # Filter out blank column names
    headers = google_sheet_data[0]
    filtered_columns = [col for col in headers if col.strip()]
    filtered_data = [filtered_columns] + [
        [cell for col, cell in zip(headers, row) if col.strip()] 
        for row in google_sheet_data[1:]
    ]

    # Create MySQL table if it doesn't exist, using filtered columns
    create_mysql_table(sheet_name, filtered_columns)

    # Sync table structure (add/remove columns)
    sync_table_structure(sheet_name, filtered_columns)

    # Sync Google Sheets data to MySQL
    sync_sheet_to_db(sheet_name, filtered_data)

    print(f"Synced Google Sheet data to MySQL at {time.ctime()}")


if __name__ == "__main__":
    sheet_id = "1p94yHH94iSnX3j6vjY5ZIUE9hyezHgz04z8HU7QkcUM"
    sheet = connect_google_sheets(sheet_id)  # Fetch the first sheet in the workbook
    sheet_name = "sheet1"  # Same name as MySQL table

    while True:
        sync_google_sheets_to_mysql(sheet, sheet_name)  # Sync Google Sheets to MySQL
        sync_db_to_google_sheets(sheet, sheet_name)     # Sync MySQL to Google Sheets
        time.sleep(10)  # Poll for changes every 10 seconds
