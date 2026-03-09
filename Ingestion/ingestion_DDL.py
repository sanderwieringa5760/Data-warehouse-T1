# Ingestion_DDL 
import pyodbc

try:
    conn = pyodbc.connect(
        "DRIVER={PostgreSQL Unicode};"
        "SERVER=localhost;"
        "PORT=5432;"
        "DATABASE=postgres;"
        "UID=postgres;"
        "PWD=Figaro123;"
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Check if the 'dwh' database exists 
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'dwh'")
    if not cursor.fetchone():
        cursor.execute("CREATE DATABASE dwh")
        print("Database 'dwh' created!")
    else:
        print("Database 'dwh' already exists.")

    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error during database initialization: {e}")

# Step 2: Connect to the newly created 'dwh' database for schema and table ingestion
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

    # Create the ingestion schema if it doesn't already exist
    cursor.execute("CREATE SCHEMA IF NOT EXISTS ingestion")
    print("Schema 'ingestion' verified/created!")

    # Define and create the CRM Customer Info table
    cursor.execute("DROP TABLE IF EXISTS ingestion.crm_cust_info")
    cursor.execute("""
        CREATE TABLE ingestion.crm_cust_info (
            cst_id INTEGER,
            cst_key VARCHAR(50),
            cst_firstname VARCHAR(50),
            cst_lastname VARCHAR(50),
            cst_marital_status VARCHAR(50),
            cst_gndr VARCHAR(50),
            cst_create_date DATE
        )
    """)
    print("Table ingestion.crm_cust_info created!")

    # Define and create the CRM Product Info table
    cursor.execute("DROP TABLE IF EXISTS ingestion.crm_prd_info")
    cursor.execute("""
        CREATE TABLE ingestion.crm_prd_info (
            prd_id INTEGER,
            prd_key VARCHAR(50),
            prd_nm VARCHAR(50),
            prd_cost INTEGER,
            prd_line VARCHAR(50),
            prd_start_dt DATE,
            prd_end_dt DATE
        )
    """)
    print("Table ingestion.crm_prd_info created!")

    # Define and create the CRM Sales Details table
    cursor.execute("DROP TABLE IF EXISTS ingestion.crm_sales_details")
    cursor.execute("""
        CREATE TABLE ingestion.crm_sales_details (
            sls_ord_num VARCHAR(50),
            sls_prd_key VARCHAR(50),
            sls_cust_id INTEGER,
            sls_order_dt INTEGER,
            sls_ship_dt INTEGER,
            sls_due_dt INTEGER,
            sls_sales DECIMAL(10,2),
            sls_quantity INTEGER,
            sls_price DECIMAL(10,2)
        )
    """)
    print("Table ingestion.crm_sales_details created!")

    # Define and create ERP-specific data tables (Customer, Location, Category)
    tables = {
        "erp_cust_az12": "cid VARCHAR(50), bdate DATE, gen VARCHAR(50)",
        "erp_loc_a101": "cid VARCHAR(50), cntry VARCHAR(50)",
        "erp_px_cat_g1v2": "id VARCHAR(50), cat VARCHAR(50), subcat VARCHAR(50), maintenance VARCHAR(50)"
    }

    for table_name, columns in tables.items():
        cursor.execute(f"DROP TABLE IF EXISTS ingestion.{table_name}")
        cursor.execute(f"CREATE TABLE ingestion.{table_name} ({columns})")
        print(f"Table ingestion.{table_name} created!")

    cursor.close()
    conn.close()
    print("\nAll tables created successfully.")

except Exception as e:
    print(f"Error during table creation: {e}")