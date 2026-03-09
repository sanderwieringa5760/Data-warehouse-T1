import pyodbc
import pandas as pd
import zipfile
import os
import urllib

# The 'r' is required here to stop the syntax warning
connection_string = (
    r'Driver={ODBC Driver 17 for SQL Server};'
    r'Server=.\SQLEXPRESS;'
    r'Database=master;'
    r'Trusted_Connection=yes;'
)

try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Create the table
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SimpleTable')
    BEGIN
        CREATE TABLE SimpleTable (ID int)
    END
""")
    conn.commit()
    
    print("Table created successfully.")

    cursor.close()
    conn.close()
except pyodbc.Error as e:
    print(f"Database error: {e}")


