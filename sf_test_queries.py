import json
import sys
import snowflake.connector
import os

# Check for JSON file path argument
if len(sys.argv) < 2:
    print("❌ Please provide the JSON file path as an argument.")
    print("Usage: python create_table_from_json.py schema.json")
    sys.exit(1)

json_path = sys.argv[1]

# Check if file exists
if not os.path.isfile(json_path):
    print(f"❌ File not found: {json_path}")
    sys.exit(1)

# Load schema from JSON
with open(json_path, "r") as f:
    schema = json.load(f)


table_name = schema["table_name"]
columns = schema["columns"]

# Build CREATE TABLE SQL
columns_sql = ",\n  ".join([f"{col} {dtype}" for col, dtype in columns.items()])
create_sql = f"CREATE OR REPLACE TABLE {table_name} (\n  {columns_sql}\n);"

print("Generated SQL:")
print(create_sql)

conn = snowflake.connector.connect(
    user='SAURABHSAKPAL2025',
    password='SaurabhSakpal2025',
    account='splftvy-sm60395',  # e.g., abcd1234.east-us-2.azure
    warehouse='saurabhswarehouse',
    database='saurabhsdb',
    schema='goldschema'
)

cur = conn.cursor()

cur.execute(create_sql)
print(f"✅ Table '{table_name}' created successfully.")

cur.close()
conn.close()