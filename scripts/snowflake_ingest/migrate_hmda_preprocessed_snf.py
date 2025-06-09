import duckdb
import pandas as pd
import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

# === Snowflake connection ===
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="HMDA_REDLINING",
    schema="SOURCE"
)

# === MotherDuck connection ===
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
duck_con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")

# === Config ===
source_table = "mart_hmda_preprocessed"
target_table = "MART_HMDA_PREPROCESSED"
chunk_size = 500_000

# === Infer row count ===
total_rows = duck_con.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
print(f"📊 Total rows to export: {total_rows}")

# === Generate DDL from first chunk
def generate_snowflake_create_table(df: pd.DataFrame, table_name: str, schema: str = "REFINED") -> str:
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
        col_defs.append(f'{col} {sf_type}')
    ddl = f'CREATE OR REPLACE TABLE {schema}.{table_name} (\n    ' + ",\n    ".join(col_defs) + "\n);"
    return ddl

# === Export loop
for i, offset in enumerate(range(59000001, total_rows + 1, chunk_size)):
    end = offset + chunk_size - 1
    print(f"🚀 Exporting rows {offset} to {end}")

    df = duck_con.execute(f"""
        SELECT * FROM {source_table}
        WHERE rn BETWEEN {offset} AND {end}
    """).fetchdf()

    df.columns = [col.lower() for col in df.columns]  # Snowflake prefers lowercase or snake_case

    # if i == 0:
    #     ddl = generate_snowflake_create_table(df, target_table)
    #     print("🛠️ Executing DDL:\n", ddl)
    #     with conn.cursor() as cur:
    #         cur.execute(ddl)
    #     print(ddl)

    success, nchunks, nrows, _ = write_pandas(
        conn, df, target_table, schema="SOURCE", database="HMDA_REDLINING", quote_identifiers=False
    )
    if success:
        print(f"✅ Inserted {nrows} rows into {target_table}")
    else:
        print("❌ Insert failed")

conn.close()
print("🎉 Export complete")
