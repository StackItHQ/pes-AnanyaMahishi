import uuid
from database.mysql_connector import connect_mysql, get_mysql_columns
import time

# Create MySQL table if it doesn't exist
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

# Sync Google Sheets data to MySQL
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

# Sync the structure of the MySQL table to match Google Sheets
def sync_table_structure(sheet_name, google_columns):
    conn = connect_mysql()
    cursor = conn.cursor()

    # Fetch existing columns in MySQL
    mysql_columns = get_mysql_columns(sheet_name)
    mysql_columns = mysql_columns[1:]  # Ignore row number column

    print("Google Sheet Columns:", google_columns)
    print("MySQL Columns:", mysql_columns)

    # 1. Handle deleted columns (drop columns from MySQL that are not in Google Sheets)
    for mysql_col in mysql_columns:
        if mysql_col not in google_columns:
            print(f"Dropping column {mysql_col} from MySQL")
            cursor.execute(f"ALTER TABLE `{sheet_name}` DROP COLUMN `{mysql_col}`;")

    # 2. Handle new columns and re-order existing columns
    previous_column = None  # To keep track of column positions
    for idx, google_col in enumerate(google_columns):
        if google_col not in mysql_columns:
            if idx == 0:
                print(f"Adding new column {google_col} to MySQL at the beginning")
                cursor.execute(f"ALTER TABLE `{sheet_name}` ADD COLUMN `{google_col}` VARCHAR(255) AFTER `row_number`;")
            else:
                print(f"Adding new column {google_col} to MySQL after `{previous_column}`")
                cursor.execute(f"ALTER TABLE `{sheet_name}` ADD COLUMN `{google_col}` VARCHAR(255) AFTER `{previous_column}`;")
        previous_column = google_col

    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Synced table structure for '{sheet_name}'")

# Sync Google Sheets to MySQL
def sync_google_sheets_to_mysql(sheet, sheet_name,google_sheet_data):

    # Extract headers and data
    headers = google_sheet_data[0]
    data = google_sheet_data[1:]

    # Handle blank and duplicate column names
    unique_headers = []
    existing_names = set()

    for header in headers:
        cleaned_header = header.strip()
        if not cleaned_header:  # Handle blank column names
            cleaned_header = f"blank_col_{uuid.uuid4().hex[:8]}"
        if cleaned_header in existing_names:
            # Handle duplicate column names
            cleaned_header = generate_unique_column_name(cleaned_header, existing_names)
        unique_headers.append(cleaned_header)
        existing_names.add(cleaned_header)

    # Prepare the data with unique headers
    # print(headers)
    # print(unique_headers)
    filtered_data = [unique_headers] + data

    # Create MySQL table if it doesn't exist, using filtered columns
    create_mysql_table(sheet_name, unique_headers)

    # Sync table structure (add/remove columns)
    sync_table_structure(sheet_name, unique_headers)

    # Sync Google Sheets data to MySQL
    sync_sheet_to_db(sheet_name, filtered_data)

    print(f"Synced Google Sheet data to MySQL at {time.ctime()}")
