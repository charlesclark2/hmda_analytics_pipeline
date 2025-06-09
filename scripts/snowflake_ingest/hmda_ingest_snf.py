import os
import requests
import zipfile
import io
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

# === Snowflake Connection Setup ===
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="HMDA_REDLINING",
    schema="SOURCE"
)

def generate_snowflake_create_table(df: pd.DataFrame, table_name: str, schema: str = "SOURCE") -> str:
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
    ddl = f'CREATE OR REPLACE TABLE {schema.upper()}.{table_name.upper()} (\n    ' + ",\n    ".join(col_defs) + "\n);"
    return ddl

def process_csv_chunks_to_snowflake(url, conn, chunksize=500_000):
    year = url.split("/")[-2]
    table_name = f"hmda_snapshot_raw_{year}"
    print(f"📦 Processing year {year} into table: {table_name}")

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for file_info in z.infolist():
            if file_info.is_dir() or not file_info.filename.endswith(".csv"):
                continue

            with z.open(file_info.filename) as raw_file:
                text_stream = io.TextIOWrapper(raw_file, encoding="utf-8")
                reader = pd.read_csv(
                    text_stream,
                    chunksize=chunksize,
                    low_memory=False,
                    dtype=str,
                    na_values=["NA", "na", "NaN", "nan", "Exempt", "exempt", "EXEMPT"]
                )

                for i, chunk in enumerate(reader):
                    chunk = chunk.replace({"NA": None, "na": None, "NaN": None})
                    print(f"🔄 Inserting chunk {i+1} into {table_name}...")

                    if i == 0:
                        ddl = generate_snowflake_create_table(chunk, f"{table_name}")
                        print(ddl)
                        with conn.cursor() as cur:
                            cur.execute(ddl)
                            print(f"🛠️ Created table {table_name}")

                    success, nchunks, nrows, _ = write_pandas(conn, chunk, table_name, schema="SOURCE", database="HMDA_REDLINING", quote_identifiers=False)
                    if not success:
                        print(f"❌ Failed to upload chunk {i+1}")
                    else:
                        print(f"✅ Inserted {nrows} rows")

# === URL List for All Years ===
url_list = [
    'https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2018/2018_public_lar_csv.zip', 
    'https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2019/2019_public_lar_csv.zip', 
    'https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2020/2020_public_lar_csv.zip', 
    'https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2021/2021_public_lar_csv.zip', 
    'https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2022/2022_public_lar_csv.zip', 
    'https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2023/2023_public_lar_csv.zip'
]

for url_item in url_list:
    try:
        process_csv_chunks_to_snowflake(url_item, conn)
    except Exception as e:
        print(f"❌ Failed to process {url_item}: {e}")

conn.close()
print("🏁 All years processed.")
