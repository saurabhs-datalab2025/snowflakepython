import json
import os

class TableCreator:
    def __init__(self, schema_path: str):
        with open(schema_path, "r") as f:
            self.schema = json.load(f)
            self.table_name = self.schema["table_name"]
            self.schema_name = self.schema["schema_name"]
            self.stage_name = self.schema["stage_name"]
            self.file_format = self.schema["file_format"]
            self.path = self.schema["path"]
            self.columns = self.schema["columns"]




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
                FROM @{self.stage_name}/{self.path}
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
        sql_create_stg_table = self.create_stg_table_fnc()
        print("Executing SQL:\n", sql_create_stg_table)
        cursor.execute(sql_create_stg_table)
        print(f"✅ Table '{self.table_name}' created successfully.")

        copy_sql = self.create_copy_into_fnc()
        print("Executing COPY INTO SQL:\n", copy_sql)
        cursor.execute(copy_sql)
        print(f"✅ Data loaded into 'STG_{self.table_name}' successfully.")


        # Step 3: Create final table
        sql_create_final_table = self.create_final_table_fnc()
        print("🏗️ Creating final table...", sql_create_final_table)
        cursor.execute(sql_create_final_table)
        print(f"✅ Final table '{self.table_name}_FINAL' created.")

        # Step 4: Insert into final table
        sql_insert_data = self.insert_into_final_fnc()
        print("📥 Inserting latest data into final table...", sql_insert_data)
        cursor.execute(sql_insert_data)
        print(f"✅ Latest records inserted into '{self.table_name}_FINAL'.")