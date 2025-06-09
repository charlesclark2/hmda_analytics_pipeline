import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Load environment variables
load_dotenv()

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="HMDA_REDLINING",
    schema="SOURCE"
)

# Create DDL from DataFrame
def create_table_from_df(df: pd.DataFrame, table_name: str, schema: str = "SOURCE"):
    dtype_mapping = {
        "object": "VARCHAR",
        "int64": "BIGINT",
        "float64": "FLOAT",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP_NTZ"
    }
    col_defs = []
    for col, dtype in df.dtypes.items():
        sf_type = dtype_mapping.get(str(dtype), "VARCHAR")
        col_defs.append(f"{col} {sf_type}")
    ddl = f'CREATE OR REPLACE TABLE {schema}.{table_name} (\n    ' + ",\n    ".join(col_defs) + "\n);"
    return ddl

# Path and chunk size
tsv_path = "/Users/charlesclark/Documents/WGU/Capstone/HMDA_Analysis/hmda_analytics_pipeline/scripts/census/data/zip_code_market_tracker.tsv000"
chunk_size = 100_000
table_name = "ZILLOW_ZIP_CODE_MARKET_TRACKER_RAW"
schema = "SOURCE"

# Process TSV in chunks
print("🚀 Starting the insert...")
reader = pd.read_csv(tsv_path, sep="\t", chunksize=chunk_size, low_memory=False)
for i, chunk in enumerate(reader):
    if i == 0:
        ddl_stmt = create_table_from_df(chunk, table_name, schema)
        with conn.cursor() as cur:
            cur.execute(ddl_stmt)
        print(f"🛠️ Table {table_name} created.")

    success, nchunks, nrows, _ = write_pandas(conn, chunk, table_name, schema=schema, database="HMDA_REDLINING", quote_identifiers=False)
    if not success:
        print(f"❌ Failed to insert chunk {i + 1}")
    elif i % 10 == 0:
        print(f"✅ Inserted chunk {i + 1}")

conn.close()
print("🏁 All chunks inserted successfully.")
