import sys
import os
from snowflake_etl.snowflake_connection import SnowflakeConnector
from snowflake_etl.stage_creator import StageCreator
from snowflake_etl.file_format_creator import FileFormatCreator
from snowflake_etl.storage_integration_creator import StorageIntegrationCreator
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

#argument for entity types
arg = schema_path.lower()


# Keyword-based routing
if "table" in arg:
    creator = TableCreator(schema_path)
    creator.create_table(cur)
elif "file_format" in arg:
    creator = FileFormatCreator(schema_path)
    creator.create_fileformat(cur)
elif "stage" in arg:
    creator = StageCreator(schema_path)
    creator.create_stage(cur)
elif "storage" in arg:
    creator = StorageIntegrationCreator(schema_path)
    creator.create_storage_integration(cur)
else:
    print(f"No matching function for argument: {arg}")

# Clean up
cur.close()
sf.close()
