import sys
import os
from snowflake_etl.snowflake_connection import SnowflakeConnector
from snowflake_etl.table_creator import TableCreator

if len(sys.argv) < 2:
    print("Usage: python create_table.py schemas/table_schema.json")
    sys.exit(1)

schema_path = sys.argv[1]
config_path = "configs/snowflake_config.json"

if not os.path.exists(schema_path):
    print(f"❌ Schema file not found: {schema_path}")
    sys.exit(1)

# Connect to Snowflake
sf = SnowflakeConnector(config_path)
conn = sf.connect()
cur = conn.cursor()

# Create table
creator = TableCreator(schema_path)
creator.create_table(cur)


# Clean up
cur.close()
sf.close()
