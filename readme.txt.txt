 To Create Table:

python create_table.py schemas/table_schema.json


To Create Stage:

 python create_stage.py entities/stage_schema.json


CREATE ROLE snowflakepythonuser;

-- Step 1: Grant privileges to a role
GRANT ALL PRIVILEGES ON database saurabhsdb TO ROLE snowflakepythonuser;

-- Step 2: Assign role to the user
GRANT ROLE snowflakepythonuser TO USER user1;

GRANT USAGE ON DATABASE saurabhsdb TO ROLE snowflakepythonuser;
use database saurabhsdb;
GRANT USAGE ON SCHEMA goldschema TO ROLE snowflakepythonuser;

-- Optional: if you want them to create tables

GRANT USAGE, CREATE TABLE, CREATE FILE FORMAT, CREATE STAGE ON SCHEMA saurabhsdb.goldschema TO ROLE snowflakepythonuser;

GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE snowflakepythonuser;

GRANT OWNERSHIP ON STORAGE INTEGRATION your_integration TO ROLE your_role REVOKE CURRENT GRANTS;


GRANT USAGE ON INTEGRATION AZURE_INTEGRATION TO ROLE snowflakepythonuser;

