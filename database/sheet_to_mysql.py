import uuid
from database.mysql_connector import connect_mysql, get_mysql_columns

# Create MySQL table if it doesn't exist
def create_mysql_table(sheet_name, columns):
    conn = connect_mysql()
    cursor = conn.cursor()

    columns = [col for col in columns if col != 'row_number']  # Exclude 'row_number'

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

    columns = google_sheet_data[0]  # The header row
    data_rows = google_sheet_data[1:]  # The actual data

    for row_number, row in enumerate(data_rows, start=1):
        placeholders = ", ".join(["%s"] * len(row))
        column_list = ", ".join([f"`{col}`" for col in columns])
        update_query = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in columns])

        insert_query = f"""
        INSERT INTO `{sheet_name}` (`row_number`, {column_list}) VALUES ({row_number}, {placeholders})
        ON DUPLICATE KEY UPDATE {update_query};
        """
        cursor.execute(insert_query, row)

    conn.commit()
    cursor.close()
    conn.close()

# Sync the structure of the MySQL table to match Google Sheets
def sync_table_structure(sheet_name, google_columns):
    conn = connect_mysql()
    cursor = conn.cursor()

    mysql_columns = get_mysql_columns(sheet_name)
    mysql_columns = mysql_columns[1:]  # Ignore 'row_number'

    previous_column = None
    for idx, google_col in enumerate(google_columns):
        if google_col not in mysql_columns:
            if idx == 0:
                cursor.execute(f"ALTER TABLE `{sheet_name}` ADD COLUMN `{google_col}` VARCHAR(255) AFTER `row_number`;")
            else:
                cursor.execute(f"ALTER TABLE `{sheet_name}` ADD COLUMN `{google_col}` VARCHAR(255) AFTER `{previous_column}`;")
        previous_column = google_col

    conn.commit()
    cursor.close()
    conn.close()

# Sync Google Sheets to MySQL
def sync_google_sheets_to_mysql(sheet, sheet_name, google_sheet_data):
    headers = google_sheet_data[0]
    data = google_sheet_data[1:]

    # Handle blank and duplicate column names
    unique_headers = []
    existing_names = set()

    for header in headers:
        cleaned_header = header.strip()
        if not cleaned_header:
            cleaned_header = f"blank_col_{uuid.uuid4().hex[:8]}"
        if cleaned_header in existing_names:
            cleaned_header = f"{cleaned_header}_{uuid.uuid4().hex[:8]}"
        unique_headers.append(cleaned_header)
        existing_names.add(cleaned_header)

    filtered_data = [unique_headers] + data

    create_mysql_table(sheet_name, unique_headers)
    sync_table_structure(sheet_name, unique_headers)
    sync_sheet_to_db(sheet_name, filtered_data)
