import pyodbc
import pandas as pd
from datetime import date

conn = pyodbc.connect(
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=.\SQLEXPRESS;"
    r"DATABASE=MyProjectDB;"
    r"Trusted_Connection=yes;"
)
conn.autocommit = True
cursor = conn.cursor()

# Create transformation schema if it doesn't exist
cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'transformation')
    BEGIN
        EXEC('CREATE SCHEMA transformation');
    END
""")

# Read from ingestion layer
df = pd.read_sql_query(sql="SELECT * FROM ingestion.erp_PX_CAT_G1V2", con=conn)

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.erp_PX_CAT_G1V2")
cursor.execute("""
    CREATE TABLE transformation.erp_PX_CAT_G1V2 (
        ID VARCHAR(50),
        CAT VARCHAR(50),
        SUBCAT VARCHAR(50),
        MAINTENANCE VARCHAR(50)
    )
""")

# Insert cleaned data into transformation layer
placeholders = ",".join(["?" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.erp_PX_CAT_G1V2 VALUES ({placeholders})", values)

print(f"Loaded {len(df)} cleaned rows into transformation.erp_PX_CAT_G1V2")

cursor.close()
conn.close()
