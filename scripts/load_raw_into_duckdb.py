import boto3
import requests
import zipfile
import io
import pandas as pd
import duckdb

def process_csv_chunks(url, duckdb_con, chunksize=500_000): 
    year = url.split('/')[-2]
    print(year)
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z: 
        count = 0
        for file_info in z.infolist(): 
            if file_info.is_dir() or not file_info.filename.endswith('.csv'):
                continue 
            with z.open(file_info.filename) as raw_file: 
                text_stream = io.TextIOWrapper(raw_file, encoding='utf-8')
                reader = pd.read_csv(
                    text_stream, 
                    chunksize=chunksize, 
                    low_memory=False, 
                    dtype=str, 
                    na_values=["NA", "na", "NaN", "nan", "Exempt", "exempt", "EXEMPT"]
                )
                for i, chunk in enumerate(reader): 
                    chunk = chunk.replace({"NA": None, "na": None, "NaN": None})
                    print(f"Inserting chunk {i+1} into duckdb")
                    if count == 0 and i == 0: 
                        try: 
                            duckdb_con.execute(f"drop table hmda_snapshot_raw_{year}")
                        except: 
                            print(f'didnt need to drop hmda_snapshot_raw_{year}')
                        print(f"Creating hmda_snapshot_raw_{year} table")
                        duckdb_con.execute(f"create or replace table hmda_snapshot_raw_{year} as select * from chunk")
                    else: 
                        duckdb_con.execute(f"insert into hmda_snapshot_raw_{year} select * from chunk")
            count += 1


url_list = [
    'https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2018/2018_public_lar_csv.zip', 
    'https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2019/2019_public_lar_csv.zip'
]

con = duckdb.connect('/Volumes/T9/hmda_project/post_model_scores.duckdb')

for x, url_item in enumerate(url_list): 
    print(f"Processing {url_item}")
    process_csv_chunks(url_item, con)

con.close()
