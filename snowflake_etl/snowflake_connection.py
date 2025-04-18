import snowflake.connector
import json

class SnowflakeConnector:
    def __init__(self, config_path: str):
        with open(config_path, "r") as f:
            self.config = json.load(f)
        self.conn = None

    def connect(self):
        self.conn = snowflake.connector.connect(
            user=self.config["user"],
            password=self.config["password"],
            account=self.config["account"],
            warehouse=self.config["warehouse"],
            database=self.config["database"],
            schema=self.config["schema"]
        )
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
