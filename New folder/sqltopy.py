import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect("C://Users//HC//Downloads//zila_data (1).db")

# Read table into a DataFrame
df = pd.read_sql_query("SELECT * FROM monthly_reports", conn)

# Save to Excel
df.to_excel("output.xlsx", index=False)

conn.close()
