import pyodbc
import csv
import os

# Connect to the Data Warehouse (dwh)
try:
    conn = pyodbc.connect(
        "DRIVER={PostgreSQL Unicode};"
        "SERVER=localhost;"
        "PORT=5432;"
        "DATABASE=dwh;"
        "UID=postgres;"
        "PWD=Figaro123;"
    )
    conn.autocommit = True
    cursor = conn.cursor()
    print("Successfully connected to the database.")
except Exception as e:
    print(f"Connection failed: {e}")
    exit()

# Define tables 
base_path = r'C:\Users\YourUsername\Downloads\datasets.DataEngineering' '

tables = [
    {
        "table": "ingestion.crm_cust_info",
        "file": os.path.join(base_path, "source_crm", "cust_info.csv"),
        "columns": 7
    },
    {
        "table": "ingestion.crm_prd_info",
        "file": os.path.join(base_path, "source_crm", "prd_info.csv"),
        "columns": 7
    },
    {
        "table": "ingestion.crm_sales_details",
        "file": os.path.join(base_path, "source_crm", "sales_details.csv"),
        "columns": 9
    },
    {
        "table": "ingestion.erp_cust_az12",
        "file": os.path.join(base_path, "source_erp", "CUST_AZ12.csv"),
        "columns": 3
    },
    {
        "table": "ingestion.erp_loc_a101",
        "file": os.path.join(base_path, "source_erp", "LOC_A101.csv"),
        "columns": 2
    },
    {
        "table": "ingestion.erp_px_cat_g1v2",
        "file": os.path.join(base_path, "source_erp", "PX_CAT_G1V2.csv"),
        "columns": 4
    },
]

# Iterate through each table configuration to load data
for t in tables:
    table_name = t["table"]
    file_path = t["file"]
    
    # Create the correct number of placeholders (?) for the SQL command
    placeholders = ",".join(["?" for _ in range(t["columns"])])

    # Truncate the table before loading new data to avoid duplicates
    cursor.execute(f"TRUNCATE TABLE {table_name}")

    # Open and read the CSV file
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip the header row
            rows_count = 0
            
            # Process each row and insert into PostgreSQL
            for row in reader:
                # Convert empty strings to None
                row = [None if val == "" else val for val in row]
                cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", row)
                rows_count += 1

        print(f"Loaded {rows_count} rows into {table_name}")
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
    except Exception as e:
        print(f"An error occurred while processing {table_name}: {e}")

# Clean up connections
cursor.close()
conn.close()
print("All tables loaded successfully!")