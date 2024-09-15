import streamlit as st
import mysql.connector
import pandas as pd

# MySQL connection
def connect_mysql():
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='2002',  # Update with your MySQL password
        database='supsql'  # Update with your database name
    )
    return connection

# Fetch data from MySQL
def fetch_data_from_mysql(table_name):
    conn = connect_mysql()
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Execute custom SQL query
def execute_query(query):
    conn = connect_mysql()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        conn.commit()
        return "Query executed successfully!"
    except Exception as e:
        conn.rollback()
        return f"Error: {str(e)}"
    finally:
        cursor.close()
        conn.close()

# Streamlit App
def app():
    st.title("SQL Database Manager")

    # Input for table name
    table_name = st.text_input("Enter the table name to display", "sheet1")

    # Display current data
    st.subheader("Current Data")
    try:
        df = fetch_data_from_mysql(table_name)
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")

    # Input for custom SQL query
    st.subheader("Run Custom SQL Query")
    sql_query = st.text_area("Enter your SQL query here")

    if st.button("Execute Query"):
        result = execute_query(sql_query)
        if "Error" in result:
            st.error(result)
        else:
            st.success(result)
            # Refresh the displayed data after successful query
            df = fetch_data_from_mysql(table_name)
            st.dataframe(df)

if __name__ == "__main__":
    app()