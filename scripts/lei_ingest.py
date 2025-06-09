import requests
import pandas as pd
from typing import List
import duckdb 

def fetch_gleif_lei_metadata(lei_list):
    """
    Fetch metadata for a list of LEIs from the GLEIF API.
    """
    base_url = "https://api.gleif.org/api/v1/lei-records/"
    metadata = []
    count = 0
    for lei in lei_list:
        response = requests.get(f"{base_url}{lei}")
        if response.status_code == 200:
            data = response.json().get("data", {})
            attributes = data.get("attributes", {})
            metadata.append({
                "lei": lei,
                "entity_name": attributes.get("entity", {}).get("legalName", {}).get("name"),
                "registration_status": attributes.get("registration", {}).get("status"),
                "legal_jurisdiction": attributes.get("entity", {}).get("legalJurisdiction"),
                "entity_category": attributes.get("entity", {}).get("entityCategory"),
                "registration_initial_date": attributes.get("registration", {}).get("initialRegistrationDate"),
                "registration_last_update": attributes.get("registration", {}).get("lastUpdateDate"),
                "headquarters_address": attributes.get("entity", {}).get("headquartersAddress", {}).get("addressLines"),
                "bic": attributes.get("bic", None)
            })
        else:
            metadata.append({
                "lei": lei,
                "entity_name": None,
                "registration_status": "Error",
                "legal_jurisdiction": None,
                "entity_category": None,
                "registration_initial_date": None,
                "registration_last_update": None,
                "headquarters_address": None,
                "bic": None
            })
        if count % 50 == 0: 
            print(f"Fetched {count + 1} records")
        count += 1
    return pd.DataFrame(metadata)

# 🔁 Example usage:
if __name__ == "__main__":
    con = duckdb.connect('/Volumes/T9/hmda_project/post_model_scores.duckdb')
    print('getting codes')
    lei_df = con.execute('select * from lei_codes').fetchdf()
    lei_list = lei_df.lei.tolist()
    print(f"Total codes: {len(lei_list)}")
    df = fetch_gleif_lei_metadata(lei_list)
    print(df.head())
    print("Creating lei table")
    con.execute("create or replace table legal_entity_identifier_raw as select * from df")
    con.close()
