import os 
import requests
import duckdb 
import pandas as pd 
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("HUD_API_TOKEN")

url = "https://www.huduser.gov/hudapi/public/usps?type=6&query=All&year=2023"

headers = {"Authorization": "Bearer {0}".format(api_key)}

print(f"Getting Data")
response = requests.get(url, headers=headers)

if response.status_code != 200: 
    print(f"failed - {response.status_code}")
else: 
    hud_df = pd.DataFrame(response.json()['data']['results'])

col_metadata = {
    'tract': 'census_tract', 
    'geoid': 'zip_code', 
    'res_ratio': 'residential_ratio', 
    'bus_ratio': 'business_ratio', 
    'oth_ratio': 'other_ratio', 
    'city': 'city_name', 
    'state': 'state_name'
}

hud_df.rename(columns=col_metadata, inplace=True)

con = duckdb.connect('/Volumes/T9/hmda_project/post_model_scores.duckdb')
con.execute('create table if not exists census_tract_to_zip_code_raw as select * from hud_df')
print(f"Stored {hud_df.shape} rows into the database")
con.close()
