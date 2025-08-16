import os
import snowflake.connector as sc


conn_params = {
    'account': 'splftvy-sm60395',
    'user': 'user1',
    'authenticator': 'SNOWFLAKE_JWT',
    'private_key_file': '/Users/saurabhsakpal2025/.snowflake_keys/rsa_key.p8',
    'warehouse': 'SAURABHSWAREHOUSE',
    'database': 'saurabhsdb',
    'schema': 'goldschema'
}


ctx = sc.connect(**conn_params)
cs = ctx.cursor()

cs.execute(
    "CREATE OR REPLACE TABLE "
    "test_table(col1 integer, col2 string)")

