import pandas as pd
from sqlalchemy import create_engine, text
import os

# 1. SETUP PATHS AND DATABASE ENGINE
output_path = r"C:\Users\sjonn\Documents\UM\data engineering\SQL.Server\transformed_data"
if not os.path.exists(output_path):
    os.makedirs(output_path)

# Connection URL for SQLAlchemy
connection_url = (
    "mssql+pyodbc://@.\SQLEXPRESS/MyProjectDB?"
    "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)
engine = create_engine(connection_url)

# 2. DEFINE THE TABLES TO TRANSFORM
tables = [
    {"table": "crm_cust_info", "keys": ["cst_id", "cst_key"]},
    {"table": "crm_prd_info", "keys": ["prd_id", "prd_key"]},
    {"table": "crm_sales_details", "keys": ["sls_ord_num", "sls_prd_key"]},
    {"table": "erp_cust_az12", "keys": ["cid"]},
    {"table": "erp_loc_a101", "keys": ["cid"]},
    {"table": "erp_px_cat_g1v2", "keys": ["id"]}
]

print("Starting Transformation Layer...")

# 3. CREATE TRANSFORMATION SCHEMA
with engine.connect() as conn:
    conn.execute(text("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transformation') EXEC('CREATE SCHEMA transformation')"))
    conn.commit()

# 4. LOOP THROUGH TABLES 
for t in tables:
    table_name = t["table"]
    key_cols = t["keys"]
    
    print(f"Processing: {table_name}...")

    # A. EXTRACTION: Read from the 'ingestion' schema
    query = f"SELECT * FROM ingestion.{table_name}"
    df = pd.read_sql_query(query, engine)

    # B. TRANSFORMATION: Cleaning
    # 1. Remove rows where keys are missing
    df = df.dropna(subset=key_cols)
    
    # 2. Remove exact duplicates
    df = df.drop_duplicates(subset=key_cols)
    
    # 3. Trim whitespace from all string columns
for t in tables:
    table_name = t["table"]
    key_cols = t["keys"]
    
    print(f"Processing: {table_name}...")

    # A. EXTRACTION
    query = f"SELECT * FROM ingestion.{table_name}"
    df = pd.read_sql_query(query, engine)

    # B. TRANSFORMATION (Cleaning)
    df = df.dropna(subset=key_cols)
    df = df.drop_duplicates(subset=key_cols)

    # This loop cleans the columns
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = df[col].str.strip()
            except:
                continue
    
    # C. LOADING (SQL)
    df.to_sql(
        name=table_name,
        con=engine,
        schema="transformation",
        if_exists="replace",
        index=False
    )

    # D. EXPORT (CSV)
    csv_file = os.path.join(output_path, f"{table_name}_transformed.csv")
    df.to_csv(csv_file, index=False)

    print(f"Successfully transformed {len(df)} rows for {table_name}.")

print("\nAll tables transformed and exported successfully!")