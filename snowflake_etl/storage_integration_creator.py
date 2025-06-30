import json

class StageCreator:
    def __init__(self, schema_path: str):
        with open(schema_path, "r") as f:
            self.schema = json.load(f)
            self.integration_name = self.schema["integration_name"]
            self.storage_provider = self.schema["storage_provider"]
            self.enabled = self.schema["enabled"]
            self.azure_tenant_id = self.schema["azure_tenant_id"]
            self.integration_type = self.schema["integration_type"]
            self.storage_allowed_locations = self.schema["storage_allowed_locations"]
            

    def build_create_storage_integration(self) -> str:
        return f"""
                CREATE OR REPLACE STORAGE INTEGRATION {self.integration_name} \n
                TYPE = {self.integration_type} \n
                STORAGE_PROVIDER = {self.storage_provider} \n
                ENABLED = {self.enabled} \n
                AZURE_TENANT_ID = {self.azure_tenant_id} \n 
                STORAGE_ALLOWED_LOCATIONS = {self.storage_allowed_locations};"""
                
                  
        
    def create_stage(self, cursor):
        sql = self.build_create_storage_integration()
        print("Executing SQL:\n", sql)
        cursor.execute(sql)
        print(f" Stage '{self.stage_name}' created successfully.")
