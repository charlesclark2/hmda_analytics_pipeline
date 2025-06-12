import os 
import snowflake.connector 
import pandas as pd 
from dotenv import load_dotenv
from sklearn.metrics import roc_auc_score


load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="HMDA_REDLINING",
    schema="SOURCE"
)

query = """
select p.activity_year, s.actual_value, s.score
from hmda_redlining.source.scored_results s
join hmda_redlining.source.mart_hmda_preprocessed p 
on s.loan_application_id = p.loan_application_id
"""

chunk_size = 500_000
data = []

print(f"Getting the data from Snowflake")
cursor = conn.cursor()
cursor.execute(query)

while True: 
    chunk = cursor.fetchmany(chunk_size)
    if not chunk: 
        break 

    df_chunk = pd.DataFrame(chunk, columns=['ACTIVITY_YEAR', 'ACTUAL_VALUE', 'SCORE'])
    data.append(df_chunk)

cursor.close()

print("Creating the dataframe")
df_all = pd.concat(data, ignore_index=True)
df_all['ACTIVITY_YEAR'] = df_all['ACTIVITY_YEAR'].astype(int)
df_all['ACTUAL_VALUE'] = df_all['ACTUAL_VALUE'].astype(int)
df_all['SCORE'] = df_all['SCORE'].astype(float)

print(f"Scoring the model")
overall_auc = roc_auc_score(df_all['ACTUAL_VALUE'], df_all['SCORE'])
print(f"Overall AUC: {overall_auc}")

yearly_auc_df = (
    df_all.groupby('ACTIVITY_YEAR')
    .apply(lambda df: pd.Series({
        "AUC_ROC": roc_auc_score(df['ACTUAL_VALUE'], df['SCORE'])
    }))
    .reset_index()
)

overall_row = pd.DataFrame([{
    "ACTIVITY_YEAR": None, 
    "AUC_ROC": overall_auc
}])

final_df = pd.concat([yearly_auc_df, overall_row], ignore_index=True)

print("Inserting into Snowflake")

success_rows = 0
for _, row in final_df.iterrows(): 
    insert_sql = """
    insert into hmda_redlining.source.metrics_model_performance (activity_year, auc_roc)
    values (%s, %s)
    """

    conn.cursor().execute(insert_sql, (
        row['ACTIVITY_YEAR'], 
        row['AUC_ROC']
    ))
    success_rows += 1

conn.close()
print(f"Logged {success_rows} to metrics_model_performance")