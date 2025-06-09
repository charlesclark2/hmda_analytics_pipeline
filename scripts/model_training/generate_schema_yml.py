import duckdb
import os 
from dotenv import load_dotenv

load_dotenv()

motherduck_token = os.getenv('MOTHERDUCK_TOKEN')

print("Connecting to motherduck")
con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")

columns = con.execute("DESCRIBE mart_preprocessed_null_checks").fetchall()
for col in columns:
    print(f"      - name: {col[0]}\n        tests:\n          - not_null")