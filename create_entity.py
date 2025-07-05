import sys
import os
from snowflake_etl.snowflake_connection import SnowflakeConnector
from snowflake_etl.stage_creator import StageCreator
from snowflake_etl.file_format_creator import FileFormatCreator
from snowflake_etl.storage_integration_creator import StorageIntegrationCreator
from snowflake_etl.table_creator import TableCreator





entity_type = sys.argv[0]
schema_path = sys.argv[1]
config_path = "configs/snowflake_config.json"

if not os.path.exists(schema_path):
    print(f"❌ Schema file not found: {schema_path}")
    sys.exit(1)

# Connect to Snowflake
sf = SnowflakeConnector(config_path)
conn = sf.connect()
cur = conn.cursor()


if "table" in entity_type.lower():
    creator = TableCreator(schema_path)
    creator.create_table(cur)
if "storage_integration" in entity_type.lower():
    creator = StageCreator(schema_path)
    creator.create_stage(cur)
if "file_format" in entity_type.lower():
    creator = StageCreator(schema_path)
    creator.create_stage(cur)

# Clean up
cur.close()
sf.close()
