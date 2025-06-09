import duckdb 
import os 
from dotenv import load_dotenv

load_dotenv()

motherduck_token = os.getenv('MOTHERDUCK_TOKEN')

print("Connecting to motherduck")
con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")

output_path = '/Volumes/T9/hmda_project/full_dataset'

if not os.path.exists(output_path):
    os.makedirs(output_path)

sql = f"""
COPY (select * from mart_hmda_preprocessed)
TO '{output_path}'
WITH (FORMAT PARQUET, PARTITION_BY (activity_year, loan_purpose_grouped));
"""
print("Moving data from motherduck to external harddrive")
con.execute(sql)
print("Successfully moved data to hard drive")