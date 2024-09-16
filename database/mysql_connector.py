import mysql.connector

# MySQL connection
def connect_mysql():
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',  # Replace with your actual MySQL password
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
