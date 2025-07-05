import json

class FileFormatCreator:
    def __init__(self, schema_path: str):
        with open(schema_path, "r") as f:
            self.schema = json.load(f)
            self.file_format = self.schema["file_format"]
            self.file_type = self.schema["file_type"]
            self.delimiter = self.schema["delimiter"]
            self.skip_header = self.schema["skip_header"]
            

    def build_create_fileformat(self) -> str:
        return f"""
                CREATE OR REPLACE file format {self.file_format} \n
                TYPE = {self.file_type} \n
                FIELD_DELIMITER = {self.delimiter} \n
                SKIP_HEADER = {self.skip_header};"""
        

    def create_fileformat(self, cursor):
        sql = self.build_create_fileformat()
        print("Executing SQL:\n", sql)
        cursor.execute(sql)
        print(f" Stage '{self.stage_name}' created successfully.")
