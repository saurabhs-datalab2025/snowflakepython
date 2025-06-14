import json

class TableCreator:
    def __init__(self, schema_path: str):
        with open(schema_path, "r") as f:
            self.schema = json.load(f)
            self.table_name = self.schema["table_name"]
            self.schema_name = self.schema["schema_name"]
            self.columns = self.schema["columns"]

    def build_create_sql(self) -> str:
        cols = ",\n  ".join([f"{col} {dtype}" for col, dtype in self.columns.items()])
        return f"CREATE OR REPLACE TABLE {self.table_name} (\n  {cols}\n);"

    def create_table(self, cursor):
        sql = self.build_create_sql()
        print("Executing SQL:\n", sql)
        cursor.execute(sql)
        print(f"✅ Table '{self.table_name}' created successfully.")
