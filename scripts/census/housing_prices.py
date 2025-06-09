import pandas as pd 
import duckdb 


con = duckdb.connect('/Volumes/T9/hmda_project/post_model_scores.duckdb')

chunk_size = 100_000

tsv_reader = pd.read_csv("/Users/charlesclark/Documents/WGU/Capstone/HMDA_Analysis/hmda_analytics_pipeline/scripts/census/data/zip_code_market_tracker.tsv000", sep="\t", chunksize=chunk_size)

print('starting the insert')
for i, chunk in enumerate(tsv_reader): 
    if_exists = "replace" if i == 0 else "append"
    if i == 0: 
        con.execute(f"create or replace table zillow_zip_code_market_tracker_raw as select * from chunk")
    else: 
        con.execute("insert into zillow_zip_code_market_tracker_raw select * from chunk")
    if i % 20 == 0: 
        print(f"Inserted chunk {i + 1}")

con.close()
