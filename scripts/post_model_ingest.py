import duckdb
import os
import glob
import pyarrow.parquet as pq

# Paths
parquet_dir = "/Volumes/T9/hmda_project/parquet_chunks"
db_path = "/Volumes/T9/hmda_project/post_model_scores.duckdb"
table_name = "mart_hmda_preprocessed"

# Connect to DuckDB
con = duckdb.connect(db_path)

def binary_search_corrupt_row(con, file_path, start, end, temp_table='testing_load'):
    con.execute(f"CREATE OR REPLACE TABLE {temp_table} AS SELECT * FROM read_parquet('{file_path}')")
    while start < end:
        mid = (start + end) // 2
        try:
            con.execute(f"SELECT COUNT(*) FROM {temp_table} WHERE rn BETWEEN {start} AND {mid}").fetchone()
            start = mid + 1  # First half succeeded
        except:
            end = mid  # Error in first half
    print(f"🚨 Corrupt rn likely at {start} in {os.path.basename(file_path)}")
    return start


# Gather valid parquet files (skip hidden)
parquet_files = [
    f for f in sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))
    if not os.path.basename(f).startswith("._")
]

# Validate and load each file
first = True
failed_files = []
for file_path in parquet_files:
    base_path = os.path.basename(file_path)
    file_split = base_path.replace('.parquet', '').split('_')
    start_partition = file_split[-2]
    end_partition = file_split[-1]
    print(f"Valid range: {start_partition} - {end_partition}")
    try:
        # Validate with PyArrow
        pq.read_table(file_path)  

        # Other validation to double check
        try:
            con.execute(f"select count(*) from read_parquet('{file_path}')").fetchone()
        except Exception as e: 
            print(f"Failed to execute query for {file_path}")
            corrupt_row = binary_search_corrupt_row(con, file_path, int(start_partition), int(end_partition))
            print(f"🧹 You may want to exclude rn = {corrupt_row} from this file.")
            failed_files.append(file_path)
            continue

        try: 
            con.execute(f"create or replace table testing_load as select * from read_parquet('{file_path}')")
            con.execute(f"select * from testing_load order by rn").fetchall()
        except Exception as e: 
            print(f"Failed to query from parquet file at {file_path} - {e}")
            corrupt_row = binary_search_corrupt_row(con, file_path, int(start_partition), int(end_partition))
            print(f"🧹 You may want to exclude rn = {corrupt_row} from this file.")
            failed_files.append(file_path) 
            continue 

        print(f"🔄 Loading: {os.path.basename(file_path)}")
        if first:
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM testing_load")
            first = False
        else:
            con.execute(f"INSERT INTO {table_name} SELECT * FROM testing_load")
        print(f"✅ Loaded: {os.path.basename(file_path)}")

    except Exception as e: 
        print(f"❌ Skipped {file_path} due to error: {e}")
        if file_path not in failed_files: 
            failed_files.append(file_path)

con.close()
print(f"🎯 Loaded {table_name} with all valid parquet files.")
print(f"All failed files: {failed_files}")