import duckdb 
import pandas as pd 
import requests 

motherduck_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImNoYXJsZXMudC5jbGFyazg5QGdtYWlsLmNvbSIsInNlc3Npb24iOiJjaGFybGVzLnQuY2xhcms4OS5nbWFpbC5jb20iLCJwYXQiOiJiOVM2NlNBQ2dEcWllOUNYel8tYzVjVUFXVFVVbEJwYXI1czQyVW9UZVRjIiwidXNlcklkIjoiMGVhNWJmYzQtMDM0NS00NWY3LWJkMWUtNDJjOWVkYmVmNGVkIiwiaXNzIjoibWRfcGF0IiwicmVhZE9ubHkiOmZhbHNlLCJ0b2tlblR5cGUiOiJyZWFkX3dyaXRlIiwiaWF0IjoxNzQ4MTkwMDI5fQ.tXV_OliWJZbNj-zJ5zgUezO3D5O7ajsI7uC_AI8iuJQ"
census_api = '170bf83ddd021687d0e321361a226c36a6d45a12'
# Connect to MotherDuck using the token
con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")

text = '''
select distinct census_tract
from mart_hmda_model_features
where median_household_income is null 
'''

missing = con.execute(text).fetchall()
missing_list = [x[0] for x in missing]

base_url = "https://api.census.gov/data/2021/acs/acs5?get=NAME,B03002_001E,B03002_003E,B03002_004E,B03002_012E,B19013_001E,B25077_001E,B15003_022E,B15003_023E,B15003_017E,B17001_002E,B22010_002E,B19083_001E"

for item in missing_list[:20]: 
    state_code = item[:2]
    county_code = item[2:5]
    print(f"tract: {item}, state: {state_code}, county: {county_code}")
    new_url = f"{base_url}&for=tract:{item}&in=state:{state_code}+county:{county_code}&key={census_api}"
    resp = requests.get(new_url)
    if resp.status_code == 200: 
        print(resp.json())
    else: 
        print(resp.status_code)
    