import pyodbc
import pandas as pd
import numpy as np

# 2) Connect to DWH
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\SQLEXPRESS;"
    "DATABASE=MyProjectDB;"
    "Trusted_Connection=yes;"
)

print("Connected successfully to the DWH DB!")
conn.autocommit = True

cursor = conn.cursor()  

df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_sales_details", con=conn)
# Step 1 — Fix the null sls_price FIRST (before any type conversion)
df['sls_price'] = df['sls_price'].where(
    df['sls_price'].notna(),
    df['sls_sales'] / df['sls_quantity']
)

# Step 2 — Convert to numeric and round (while still NaN, not None)
df['sls_price']    = pd.to_numeric(df['sls_price'],    errors='coerce').round(2)
df['sls_sales']    = pd.to_numeric(df['sls_sales'],    errors='coerce').round(2)
df['sls_quantity'] = pd.to_numeric(df['sls_quantity'], errors='coerce')

# Step 3 — Only NOW replace NaN → None (right before inserting)
df = df.where(pd.notnull(df), None)

# Step 4 — Force native Python floats (fixes numpy type issue with pyodbc)
df['sls_price']    = df['sls_price'].apply(lambda x: float(x) if x is not None else None)
df['sls_sales']    = df['sls_sales'].apply(lambda x: float(x) if x is not None else None)
df['sls_quantity'] = df['sls_quantity'].apply(lambda x: float(x) if x is not None else None)

print(df)

# Explicitly cast to string, then slice the first 5 characters
# df['sls_quantity'] = df['sls_quantity'].replace("NULL", "sls_sales/sls_quantity)
# df["cat_id"] = df["cat_id"].str.replace("-", "_")
# df["prd_cost"] = df["prd_cost"].fillna(0)
# df["prd_line"] = df["prd_line"].str.replace("R", "Road") 
# df["prd_line"] = df["prd_line"].str.replace("S", "Sport")
# df["prd_line"] = df["prd_line"].str.replace("M", "Mountain")
# df["prd_line"] = df["prd_line"].str.replace("T", "Touring")
# df["prd_line"] = df["prd_line"].fillna("N/A")

# This takes the start date of the next row and puts it into the end date of the current row
# df["prd_end_dt"] = df["prd_start_dt"].shift(-1)
# df["prd_start_dt"] = pd.to_datetime(df["prd_start_dt"])
# # Shift the start dates up, convert to datetime, and subtract 1 day
# df["prd_end_dt"] = pd.to_datetime(df["prd_start_dt"].shift(-1)) - pd.Timedelta(days=1)

# df["prd_start_dt"] = pd.to_datetime(df["prd_start_dt"])
# df = df.sort_values(["prd_key", "prd_start_dt"])
# df["prd_end_dt"] = df.groupby("prd_key")["prd_start_dt"].shift(-1) - pd.Timedelta(days=1)

# df["prd_start_dt"] = pd.to_datetime(df["prd_start_dt"], errors='coerce')
# df["prd_end_dt"] = pd.to_datetime(df["prd_end_dt"], errors='coerce')

# df = df.sort_values(["prd_key", "prd_start_dt"])
# df["prd_end_dt"] = df.groupby("prd_key")["prd_start_dt"].shift(-1) - pd.Timedelta(days=1)

# df= df.sort_values("prd_id").copy()
# print(df)


# df = df.dropna(subset=["prd_id"])
# df = df.drop_duplicates(subset=['prd_id'], keep='last')

# df["prd_key"] = df['prd_key'].str.strip()
# df["prd_nm"] = df['prd_nm'].str.strip()

# df["cst_marital_status"] = df["cst_marital_status"].replace("S", "Single")
# df["cst_marital_status"] = df["cst_marital_status"].replace("M", "Married")
# df["cst_marital_status"] = df["cst_marital_status"].fillna("N/A")

# df["cst_gndr"] = df["cst_gndr"].replace("M", "Male")
# df["cst_gndr"] = df["cst_gndr"].replace("F", "Female")
# df["cst_gndr"] = df["cst_gndr"].fillna("N/A")

# df["cst_create_date"] = pd.to_datetime(df["cst_create_date"])
# df["cst_id"] = pd.to_numeric(df["cst_id"]).astype(int)


cursor.execute("""
TRUNCATE TABLE transformation.crm_sales_details;
""")
sql = """
INSERT INTO transformation.crm_sales_details
(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
cursor.fast_executemany = False  # Important!
rows = df.values.tolist()
cursor.executemany(sql, rows)
conn.commit()


print("Inserted")
cursor.close()
conn.close()