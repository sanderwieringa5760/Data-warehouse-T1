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

df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_prd_info", con=conn)

# create a new column containing the first five characters of prd_key
# ensure the column is treated as string before slicing


# Explicitly cast to string, then slice the first 5 characters
# df['cat_id'] = df['prd_key'].str[:5]
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
TRUNCATE TABLE transformation.erp.PX_CAT_G1V2;
""")
sql = """
INSERT INTO transformation.erp.PX_CAT_G1V2
(ID,CAT,SUBCAT,MAINTENANCE)
VALUES (?, ?, ?, ?)
"""
cursor.fast_executemany = True

rows = df.itertuples(index=False, name=None)

cursor.executemany(sql, rows)
conn.commit()


print("Inserted")
cursor.close()
conn.close()