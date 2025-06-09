import os
import requests
import pandas as pd
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

# === Load environment variables ===
load_dotenv()
api_key = os.getenv("HUD_API_TOKEN")
sf_user = os.getenv("SNOWFLAKE_USER")
sf_password = os.getenv("SNOWFLAKE_PASSWORD")
sf_account = os.getenv("SNOWFLAKE_ACCOUNT")  
sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
sf_database = os.getenv("SNOWFLAKE_DATABASE")
sf_schema = "SOURCE"  

# === Step 1: Request the HUD API ===
url = "https://www.huduser.gov/hudapi/public/usps?type=6&query=All&year=2023"
headers = {"Authorization": f"Bearer {api_key}"}

print("📡 Fetching data from HUD API...")
response = requests.get(url, headers=headers)

if response.status_code != 200:
    raise RuntimeError(f"❌ Failed to fetch data: {response.status_code}")

hud_df = pd.DataFrame(response.json()['data']['results'])
print(hud_df.columns.tolist())

# === Step 2: Rename columns ===
col_metadata = {
    'tract': 'census_tract',
    'geoid': 'zip_code',
    'res_ratio': 'residential_ratio',
    'bus_ratio': 'business_ratio',
    'oth_ratio': 'other_ratio',
    'tot_ratio': 'total_ratio',
    'city': 'city_name',
    'state': 'state_name'
}
hud_df.rename(columns=col_metadata, inplace=True)

# === Step 3: Connect to Snowflake ===
print("🔐 Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)

# === Step 4: Upload the data ===
target_table = "CENSUS_TRACT_TO_ZIP_CODE_RAW"

print(f"⬆️ Uploading {len(hud_df)} rows to {sf_database}.{sf_schema}.{target_table}...")

# Optional: Create table if it doesn't exist
create_stmt = f"""
CREATE OR REPLACE TABLE {sf_schema}.{target_table} (
    census_tract VARCHAR,
    zip_code VARCHAR,
    residential_ratio FLOAT,
    business_ratio FLOAT,
    other_ratio FLOAT,
    total_ratio FLOAT,
    city_name VARCHAR,
    state_name VARCHAR
);
"""
conn.cursor().execute(create_stmt)

print(hud_df.columns.tolist())
# Upload using write_pandas
success, nchunks, nrows, _ = write_pandas(
    conn,
    hud_df,
    table_name=target_table,
    schema=sf_schema,
    database=sf_database,
    quote_identifiers=False
)

if success:
    print(f"✅ Uploaded {nrows} rows across {nchunks} chunks to {target_table}")
else:
    print("❌ Upload failed")

conn.close()
