import zipfile
import requests 
import geopandas as gpd
import duckdb 
import io


# con = duckdb.connect('/Volumes/T9/hmda_project/post_model_scores.duckdb')


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

tract_centroids = download_and_extract_tract_centroids()
print(type(tract_centroids))

# text = '''
# create or replace table tract_centroids_raw
# as 
# select *
# from tract_centroids
# '''
# con.execute(text)
