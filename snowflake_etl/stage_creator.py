import json

class StageCreator:
    def __init__(self, schema_path: str):
        with open(schema_path, "r") as f:
            self.schema = json.load(f)
            self.stage_name = self.schema["stage_name"]
            self.integration_name = self.schema["integration_name"]
            self.stage_url = self.schema["stage_url"]
            self.file_format = self.schema["file_format"]
            

    def build_create_stage(self) -> str:
        return f"""
                CREATE OR REPLACE STAGE {self.stage_name} \n
                STORAGE_INTEGRATION = {self.integration_name} \n
                URL = {self.stage_url} \n
                FILE_FORMAT = {self.file_format};"""
                
    
    def create_stage(self, cursor):
        sql = self.build_create_stage()
        print("Executing SQL:\n", sql)
        cursor.execute(sql)
        print(f" Stage '{self.stage_name}' created successfully.")
