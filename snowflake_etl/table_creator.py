import json
import os

class TableCreator:
    def __init__(self, schema_path: str):
        with open(schema_path, "r") as f:
            self.schema = json.load(f)
            self.table_name = self.schema["table_name"]
            self.schema_name = self.schema["schema_name"]
<<<<<<< HEAD
=======
            self.stage_name = self.schema["stage_name"]
            self.file_format = self.schema["file_format"]
>>>>>>> feature_create_stage
            self.columns = self.schema["columns"]

        # ✅ Build path to configs/snowflake_config.json
        current_dir = os.path.dirname(os.path.abspath(__file__))  # this points to the snowflake_etl/ folder
        root_dir = os.path.dirname(current_dir)  # go up to project root (snowflakepython/)
        config_path = os.path.join(root_dir, "configs", "snowflake_config.json")

        with open(config_path) as f:
            self.config = json.load(f)
        



    def create_stg_table_fnc(self) -> str:
        cols = ",\n  ".join([f"{col} {dtype}" for col, dtype in self.columns.items()])
        cols += ",\n  LOAD_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()"
        return f"""
                CREATE OR REPLACE TABLE STG_{self.table_name} (
                {cols}
                );
                """.strip()

    def create_copy_into_fnc(self) -> str:
        # Exclude LOAD_TS from the COPY INTO column list
        column_list = ", ".join(self.columns.keys())
        return f"""
                COPY INTO STG_{self.table_name} ({column_list})
                FROM @{self.stage_name}
                FILE_FORMAT = (FORMAT_NAME = {self.file_format})
                ON_ERROR = 'CONTINUE';
                """.strip()
    
    def create_final_table_fnc(self) -> str:
        cols = ",\n  ".join([f"{col} {dtype}" for col, dtype in self.columns.items()])
        cols += ",\n  load_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()"
        return f"""
            CREATE OR REPLACE TABLE {self.table_name}_FINAL (
              {cols}
            );
        """.strip()

    def insert_into_final_fnc(self) -> str:
        column_list = ", ".join(self.columns.keys()) + ", load_ts"
        return f"""
            INSERT INTO {self.table_name}_FINAL ({column_list})
            SELECT {column_list}
            FROM STG_{self.table_name}
            WHERE load_ts = (SELECT MAX(load_ts) FROM STG_{self.table_name});
        """.strip()


    def create_table(self, cursor):
        sql = self.create_stg_table_fnc()
        print("Executing SQL:\n", sql)
        cursor.execute(sql)
        print(f"✅ Table '{self.table_name}' created successfully.")

        copy_sql = self.create_copy_into_fnc()
        print("Executing COPY INTO SQL:\n", copy_sql)
        cursor.execute(copy_sql)
        print(f"✅ Data loaded into 'STG_{self.table_name}' successfully.")


        # Step 3: Create final table
        print("🏗️ Creating final table...")
        cursor.execute(self.create_final_table_fnc())
        print(f"✅ Final table '{self.table_name}_FINAL' created.")

        # Step 4: Insert into final table
        print("📥 Inserting latest data into final table...")
        cursor.execute(self.insert_into_final_fnc())
        print(f"✅ Latest records inserted into '{self.table_name}_FINAL'.")