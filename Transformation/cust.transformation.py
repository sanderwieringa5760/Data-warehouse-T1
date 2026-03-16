import pyodbc
import pandas as pd

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

df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_cust_info", con=conn)

df = df.dropna(subset=["cst_id"])
df = df.drop_duplicates(subset=['cst_id'], keep='last')

df["cst_firstname"] = df['cst_firstname'].str.strip()
df["cst_lastname"] = df['cst_lastname'].str.strip()

df["cst_marital_status"] = df["cst_marital_status"].replace("S", "Single")
df["cst_marital_status"] = df["cst_marital_status"].replace("M", "Married")
df["cst_marital_status"] = df["cst_marital_status"].fillna("N/A")

df["cst_gndr"] = df["cst_gndr"].replace("M", "Male")
df["cst_gndr"] = df["cst_gndr"].replace("F", "Female")
df["cst_gndr"] = df["cst_gndr"].fillna("N/A")

df["cst_create_date"] = pd.to_datetime(df["cst_create_date"])
df["cst_id"] = pd.to_numeric(df["cst_id"]).astype(int)




cursor.execute("""
TRUNCATE TABLE transformation.crm_cust_info;
""")
sql = """
INSERT INTO transformation.crm_cust_info
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""
cursor.fast_executemany = True

rows = df.itertuples(index=False, name=None)

cursor.executemany(sql, rows)
conn.commit()


print("Inserted")
cursor.close()
conn.close()