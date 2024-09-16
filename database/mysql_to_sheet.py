from database.mysql_connector import connect_mysql

# Fetch data from MySQL
def fetch_mysql_data(sheet_name):
    conn = connect_mysql()
    cursor = conn.cursor()

    cursor.execute(f"SHOW COLUMNS FROM `{sheet_name}`")
    columns = [col[0] for col in cursor.fetchall() if col[0] != 'row_number']
    
    column_list = ", ".join(f"`{col}`" for col in columns)
    query = f"SELECT {column_list} FROM `{sheet_name}`"

    cursor.execute(query)
    data = cursor.fetchall()

    data_with_columns = [columns] + data

    cursor.close()
    conn.close()
    
    return data_with_columns

# Update Google Sheets with MySQL data
def update_google_sheets(sheet, new_data, existing_data):
    existing_headers = existing_data[0]
    new_headers = new_data[0]

    if existing_headers != new_headers:
        sheet.resize(len(new_data) + 1, len(new_headers))
        sheet.update('A1', [new_headers])

    start_row = 2
    for row_number, new_row in enumerate(new_data[1:], start=start_row):
        existing_row = existing_data[row_number - 1] if row_number <= len(existing_data) else None

        if not existing_row or existing_row != new_row:
            cell_range = f"A{row_number}:Z{row_number}"  # Adjust column range as necessary
            sheet.update(cell_range, [new_row])

# Sync MySQL data to Google Sheets
def sync_db_to_google_sheets(sheet, sheet_name, google_sheet_data):
    mysql_data = fetch_mysql_data(sheet_name)
    update_google_sheets(sheet, mysql_data, google_sheet_data)
