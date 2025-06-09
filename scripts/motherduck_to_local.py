import pandas as pd 
import duckdb 

source_dir = '/Users/charlesclark/Documents/WGU/Capstone/HMDA_Analysis/hmda_analytics_pipeline/scripts/census/data/motherduck/'

file_list = [
    'feature_importances_log', 
    'hmda_categories', 
    'shap_values_log', 
    'state_fips_xref'
]

con = duckdb.connect('/Volumes/T9/hmda_project/post_model_scores.duckdb')

for file in file_list: 
    file_path = f"{source_dir}{file}.csv"
    df = pd.read_csv(file_path)
    if file == 'hmda_categories': 
        file = 'hmda_categories_xref'
    con.execute(f"create table if not exists {file} as select * from df limit 0")
    con.execute(f"insert into {file} select * from df")
    print(f"Loaded {file} into duckdb")