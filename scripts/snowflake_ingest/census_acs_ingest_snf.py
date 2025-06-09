import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv
import os

# === Load credentials ===
load_dotenv()
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = "HMDA_REDLINING"
schema = "SOURCE"

# === State FIPS reference ===
url = "https://www2.census.gov/geo/docs/reference/state.txt"
state_fips_df = pd.read_csv(url, sep="|", header=0, names=["fips", "usps", "state_name", "gnisid"], dtype=str)
state_fips_list = state_fips_df.fips.tolist()

# === ACS configuration ===
census_api_key = os.getenv("CENSUS_API_KEY")
acs_variable_dict = {
    "B03002_001E": "total_population", 
    "B03002_003E": "white_population", 
    "B03002_004E": "black_population", 
    "B03002_012E": "latinx_population", 
    "B19013_001E": "median_household_income", 
    "B25077_001E": "median_home_value", 
    "B15003_022E": "bachelors_degree_population", 
    "B15003_023E": "graduate_degree_population", 
    "B15003_017E": "high_school_diploma_population", 
    "B17001_002E": "total_population_below_poverty_level", 
    "B22010_002E": "total_population_receiving_federal_assistance", 
    "B19083_001E": "gini_index_of_income_inequality"
}
acs_fields = list(acs_variable_dict.keys())

# === Snowflake connection ===
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
)
cur = conn.cursor()

# === Process each state's data ===
base_url = "https://api.census.gov/data/2021/acs/acs5"
headers = {
    "User-Agent": "Mozilla/5.0 (compatible; CensusDataBot/1.0; +https://example.com)"
}
count = 0

for state in state_fips_list:
    try:
        print(f"🌎 Processing state {state}")
        params = {
            "get": "NAME," + ",".join(acs_fields),
            "for": "tract:*",
            "in": f"state:{state}",
            "key": census_api_key
        }
        response = requests.get(base_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        rows = response.json()
        header, data = rows[0], rows[1:]

        sample_df = pd.DataFrame(data, columns=header)
        sample_df.rename(columns=acs_variable_dict, inplace=True)

        # Convert numerics
        numeric_cols = list(acs_variable_dict.values())
        sample_df[numeric_cols] = sample_df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        # Create table once
        if count == 0:
            col_defs = []
            for col in sample_df.columns:
                dtype = sample_df[col].dtype
                if pd.api.types.is_float_dtype(dtype):
                    col_defs.append(f"{col} FLOAT")
                elif pd.api.types.is_integer_dtype(dtype):
                    col_defs.append(f"{col} INTEGER")
                else:
                    col_defs.append(f"{col} VARCHAR")
            column_ddl = ",\n    ".join(col_defs)
            ddl = f"CREATE OR REPLACE TABLE {schema}.census_acs_raw (\n    {column_ddl}\n);"
            cur.execute(ddl)

        # Insert data in batches
        for i in range(0, len(sample_df), 1000):
            batch = sample_df.iloc[i:i+1000]
            placeholders = ','.join(['%s'] * len(batch.columns))
            insert_sql = f"""
                INSERT INTO {schema}.census_acs_raw ({','.join(batch.columns)})
                VALUES ({placeholders})
            """
            cur.executemany(insert_sql, batch.values.tolist())

        print(f"✅ Inserted {len(sample_df)} rows for state {state}")
        count += 1

    except Exception as e:
        print(f"❌ Failed for state {state}: {e}")
        continue

cur.close()
conn.close()
print("🎯 All states processed and inserted into Snowflake.")
