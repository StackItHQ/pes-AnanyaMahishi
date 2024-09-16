from database.mysql_connector import connect_mysql
import time
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

# Update Google Sheets with MySQL data
def update_google_sheets(sheet, new_data,existing_data):
    
    # Assuming first row is header
    existing_headers = existing_data[0]#google sheets
    new_headers = new_data[0]#mysql
    # print("OLD HEADER:",existing_headers)
    # print("NEW HEADER:",new_headers)
    
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
def sync_db_to_google_sheets(sheet, sheet_name,google_sheet_data):
    # Fetch MySQL data
    mysql_data = fetch_mysql_data(sheet_name)

    # Update Google Sheets
    update_google_sheets(sheet, mysql_data,google_sheet_data)

    print(f"Synced MySQL data to Google Sheets at {time.ctime()}")
