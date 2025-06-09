import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Snowflake connection parameters
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    role='ACCOUNTADMIN'
)

# conn = snowflake.connector.connect(
#     user='hmda_script_user', 
#     password='Cc283192$$!!$$!!', 
#     account='ihupics-dp59975', 
#     warehouse='COMPUTE_WH', 
#     database='HMDA_REDLINING', 
#     schema='SOURCE', 
#     role='ACCOUNTADMIN'
# )

cursor = conn.cursor()

# Paths
source_dir = '/Users/charlesclark/Documents/WGU/Capstone/HMDA_Analysis/hmda_analytics_pipeline/scripts/census/data/motherduck/'

file_list = [
    'feature_importances_log', 
    'hmda_categories', 
    'shap_values_log', 
    'state_fips_xref'
]

for file in file_list:
    file_path = f"{source_dir}{file}.csv"
    df = pd.read_csv(file_path)

    # Rename table if needed
    table_name = 'hmda_categories_xref' if file == 'hmda_categories' else file

    print(f"📦 Loading {table_name} into Snowflake...")

    # Drop and recreate table (optional for clean load)
    create_table_stmt = f"CREATE OR REPLACE TABLE {table_name} ("
    create_table_stmt += ', '.join([f'"{col}" VARCHAR' for col in df.columns])
    create_table_stmt += ")"
    cursor.execute(create_table_stmt)

    # Upload data
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
    print(f"✅ Loaded {nrows} rows into {table_name.upper()}")

# Cleanup
cursor.close()
conn.close()
