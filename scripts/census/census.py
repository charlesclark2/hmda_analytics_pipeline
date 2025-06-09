import pandas as pd 
import numpy as np 
import duckdb 
import requests 

url = "https://www2.census.gov/geo/docs/reference/state.txt"

# Read the file into a pandas DataFrame
state_fips_df = pd.read_csv(url, sep="|", header=0, names=["fips", "usps", "state_name", "gnisid"], dtype=str)

census_api_key = '170bf83ddd021687d0e321361a226c36a6d45a12'
state_fips_list = state_fips_df.fips.tolist()

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

all_data = []
params = {
    "get": "NAME," + ",".join(acs_fields),
    "for": "tract:*",
    "in": "state:01" ,
    "key": census_api_key
}
headers = {
    "User-Agent": "Mozilla/5.0 (compatible; CensusDataBot/1.0; +https://example.com)"
}
base_url = "https://api.census.gov/data/2021/acs/acs5"

con = duckdb.connect('/Volumes/T9/hmda_project/post_model_scores.duckdb')

count = 0
for state in state_fips_list: 
    # test = con.execute(f"select count(*) from census_acs_raw where state like '{state}'").fetchall()
    # print(test[0][0])
    # if test[0][0] < 1:
    try:
        print(f"Starting process for state {state}")
        params['in'] = f"state:{state}"
        assert isinstance(headers, dict), "headers must be a dictionary"
        response = requests.get(base_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        rows = response.json()
        row_header, data = rows[0], rows[1:]
        print(f"Processed state {state}")
        sample_df = pd.DataFrame(data, columns=row_header)
        sample_df = sample_df.rename(columns=acs_variable_dict)
        numeric_cols = list(acs_variable_dict.values())
        sample_df[numeric_cols] = sample_df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        if count == 0: 
            try:
                con.execute("drop table census_acs_raw")
            except: 
                print("Alerady dropped the table")
            con.execute("create table if not exists census_acs_raw as select * from sample_df limit 0")
        con.execute("insert into census_acs_raw select * from sample_df")
        print(f"Inserted {sample_df.shape} rows of data")
    except: 
        print(f"failed the api call for {state}")
    
        
    count += 1
