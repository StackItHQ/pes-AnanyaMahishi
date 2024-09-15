import streamlit as st
import mysql.connector
import pandas as pd

# MySQL connection
def connect_mysql():
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',  # Update with your MySQL password
        database='testsql'  # Update with your database name
    )
    return connection

# Fetch data from MySQL
def fetch_data_from_mysql(table_name):
    conn = connect_mysql()
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Insert data into MySQL
def insert_data_to_mysql(table_name, data):
    conn = connect_mysql()
    cursor = conn.cursor()

    placeholders = ", ".join(["%s"] * len(data))
    columns = ", ".join(data.keys())
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    cursor.execute(insert_query, tuple(data.values()))
    conn.commit()
    cursor.close()
    conn.close()

# Update data in MySQL
def update_data_in_mysql(table_name, data, row_id):
    conn = connect_mysql()
    cursor = conn.cursor()

    update_query = ", ".join([f"{k} = %s" for k in data.keys()])
    query = f"UPDATE {table_name} SET {update_query} WHERE id = %s"
    
    cursor.execute(query, tuple(data.values()) + (row_id,))
    conn.commit()
    cursor.close()
    conn.close()

# Delete data from MySQL
def delete_data_from_mysql(table_name, row_id):
    conn = connect_mysql()
    cursor = conn.cursor()

    delete_query = f"DELETE FROM {table_name} WHERE id = %s"
    cursor.execute(delete_query, (row_id,))
    
    conn.commit()
    cursor.close()
    conn.close()

# Streamlit App
def app():
    st.title("MySQL Database Management")

    table_name = st.text_input("Enter the table name", "sheet1")

    # Display current data
    st.subheader("View Data")
    df = fetch_data_from_mysql(table_name)
    st.dataframe(df)

    # Insert new data
    st.subheader("Insert Data")
    new_data = {}
    for col in df.columns:
        if col != 'id':  # Assuming 'id' is auto-incremented
            new_data[col] = st.text_input(f"Enter {col}", "")

    if st.button("Insert Data"):
        insert_data_to_mysql(table_name, new_data)
        st.success("Data inserted successfully!")
        st.experimental_rerun()

    # Update existing data
    st.subheader("Update Data")
    update_id = st.number_input("Enter ID to update", min_value=0, step=1)
    update_data = {}
    for col in df.columns:
        if col != 'id':  # Assuming 'id' is auto-incremented
            update_data[col] = st.text_input(f"New value for {col}", "")

    if st.button("Update Data"):
        if update_id > 0:
            update_data_in_mysql(table_name, update_data, update_id)
            st.success("Data updated successfully!")
            st.experimental_rerun()
        else:
            st.warning("Please enter a valid ID.")

    # Delete existing data
    st.subheader("Delete Data")
    delete_id = st.number_input("Enter ID to delete", min_value=0, step=1)

    if st.button("Delete Data"):
        if delete_id > 0:
            delete_data_from_mysql(table_name, delete_id)
            st.success("Data deleted successfully!")
            st.experimental_rerun()
        else:
            st.warning("Please enter a valid ID.")

if __name__ == "__main__":
    app()
