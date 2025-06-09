import zipfile
import requests 
import geopandas as gpd
import io
import os
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


def generate_snowflake_create_table(df: pd.DataFrame, table_name: str, schema: str = "PUBLIC") -> str:
    dtype_mapping = {
        "object": "VARCHAR",
        "int64": "BIGINT",
        "float64": "FLOAT",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP_NTZ",
    }

    col_defs = []
    for col, dtype in df.dtypes.items():
        sf_type = dtype_mapping.get(str(dtype), "VARCHAR")
        col_defs.append(f'{col} {sf_type}')  # <-- No quotes around column name

    ddl = f'CREATE OR REPLACE TABLE {schema}.{table_name} (\n    ' + ",\n    ".join(col_defs) + "\n);"
    return ddl


def download_and_extract_tract_centroids(resolution="500k", year="2020"):
    # URL for cartographic boundary shapefiles
    url = f"https://www2.census.gov/geo/tiger/GENZ{year}/shp/cb_{year}_us_tract_{resolution}.zip"
    print(f"🔽 Downloading from {url}...")

    # Download and unzip in memory
    response = requests.get(url, verify=False)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        zf.extractall("cb_tract_shapefiles")

    # Load shapefile
    shp_path = "cb_tract_shapefiles/cb_2020_us_tract_500k.shp"
    gdf = gpd.read_file(shp_path)

    # Ensure geometry is in lat/lon (EPSG:4326)
    gdf = gdf.to_crs("EPSG:4326")

    # Extract centroid coordinates
    gdf["centroid_lat"] = gdf.geometry.centroid.y
    gdf["centroid_lon"] = gdf.geometry.centroid.x

    # Clean columns
    gdf["state_fips"] = gdf["STATEFP"]
    gdf["county_fips"] = gdf["COUNTYFP"]
    gdf["tract_code"] = gdf["TRACTCE"]
    gdf["tract_fips"] = gdf["GEOID"]

    df = gdf[["tract_fips", "state_fips", "county_fips", "tract_code", "centroid_lat", "centroid_lon"]].copy()
    return df

print("🔐 Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)

tract_centroids = download_and_extract_tract_centroids()
table_sql = generate_snowflake_create_table(tract_centroids, 'tract_centroids_raw', 'source')
print(table_sql)

# Creating the table
conn.cursor().execute(table_sql)

# Upload using write_pandas
success, nchunks, nrows, _ = write_pandas(
    conn,
    tract_centroids,
    table_name='tract_centroids_raw',
    schema=sf_schema,
    database=sf_database,
    quote_identifiers=False
)

if success:
    print(f"✅ Uploaded {nrows} rows across {nchunks} chunks to tract_centroids_raw")
else:
    print("❌ Upload failed")

conn.close()

