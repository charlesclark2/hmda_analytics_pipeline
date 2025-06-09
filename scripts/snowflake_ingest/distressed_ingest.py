import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# === Load credentials ===
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

# === Processing Function ===
def process_distressed_tracts(input_csv_path):
    df = pd.read_csv(input_csv_path, skiprows=[0, 1], header=0)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    df["state_code"] = pd.to_numeric(df["state_code"], errors="coerce")
    df["county_code"] = pd.to_numeric(df["county_code"], errors="coerce")
    df["tract_code"] = df["tract_code"].astype(str).str.replace(".", "", regex=False)
    df["tract_code"] = pd.to_numeric(df["tract_code"], errors="coerce")
    df = df.dropna(subset=["state_code", "county_code", "tract_code"]).copy()
    df["state_code"] = df["state_code"].astype(int).astype(str).str.zfill(2)
    df["county_code"] = df["county_code"].astype(int).astype(str).str.zfill(3)
    df["tract_code"] = df["tract_code"].astype(int).astype(str).str.zfill(6)
    df["tract_fips"] = df["state_code"] + df["county_code"] + df["tract_code"]

    if 'distressed' not in df.columns.tolist(): 
        df["is_distressed"] = df[["poverty", "unemployment", "population_loss"]].fillna("").eq("X").any(axis=1).astype(int)
        df["is_underserved"] = df["remote_rural"].fillna("").eq("X").astype(int)
    else:
        df["is_distressed"] = df["distressed"].fillna("").str.upper().eq("X").astype(int)
        df["is_underserved"] = df["under_served"].fillna("").str.upper().eq("X").astype(int)

    return df

# === Build Full DataFrame ===
dfs = []
for year in range(2018, 2024):
    file_path = f"/Users/charlesclark/Documents/WGU/Capstone/HMDA_Analysis/hmda_analytics_pipeline/scripts/census/data/{year}distressedorunderservedtracts.csv"
    df = process_distressed_tracts(file_path)
    df['year'] = int(year)
    df = df[['year', 'county_name', 'state_name', 'poverty', 'unemployment', 'population_loss', 'remote_rural',
             'state_code', 'county_code', 'tract_code', 'tract_fips', 'is_distressed', 'is_underserved']]
    dfs.append(df)

all_years_df = pd.concat(dfs, ignore_index=True)
all_years_df.rename(columns={"year": "report_year"}, inplace=True)
print(f"All years row count: {all_years_df.shape[0]}")

# === Create Table if Not Exists ===
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

ddl_stmt = create_table_from_df(all_years_df, "DISTRESSED_CENSUS_TRACTS_RAW")
with conn.cursor() as cur:
    cur.execute(ddl_stmt)
    print("✅ Table created in Snowflake")

# === Upload Data ===
success, nchunks, nrows, _ = write_pandas(conn, all_years_df, "DISTRESSED_CENSUS_TRACTS_RAW", schema="SOURCE", database="HMDA_REDLINING", quote_identifiers=False)
if success:
    print(f"✅ Inserted {nrows} rows into Snowflake table.")
else:
    print("❌ Upload failed")

conn.close()
