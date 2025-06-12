import os 
import snowflake.connector 
import pandas as pd 
from dotenv import load_dotenv
from catboost import CatBoostClassifier
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="HMDA_REDLINING",
    schema="SOURCE"
)

model = CatBoostClassifier()
model.load_model("hmda_catboost_final_model.cbm")

sql_text = """
select p.*, s.score as previous_score
from hmda_redlining.source.scored_results s 
join hmda_redlining.source.mart_hmda_post_model_set p 
on s.loan_application_id = p.loan_application_id
where s.score between 0.4 and 0.6
and s.predicted_label = 0
and s.applicant_derived_racial_category not like 'Unknown'
"""
df = pd.read_sql(sql_text, conn)

selected_cols = [
    'activity_year', 'dti_bin', 'lender_prior_year_approval_rate',
    'loan_purpose_grouped', 'property_value', 'lender_prior_approval_rate_white',
    'loan_type', 'loan_amount', 'lender_prior_approval_rate_black',
    'lender_prior_approval_rate_latinx', 'lender_prior_approval_rate_aapi',
    'lender_minority_gap', 'income_log_x_property_value', 'prior_approval_rate',
    'income_to_loan_ratio_stratified', 'applicant_age', 'income_log', 'income',
    'applicant_derived_racial_category', 'race_state_interaction',
    'distressed_or_underserved_race', 'avg_median_price_per_square_foot',
    'pct_bachelors_or_higher', 'loan_to_income_ratio', 'lender_registration_status',
    'minority_population_pct', 'gini_index_of_income_inequality',
    'tract_minority_gap', 'gini_x_income_log'
]

df.columns = df.columns.str.lower()
original_indices = df.index.tolist()
df_model = df[selected_cols]


categorical_cols = [
    'dti_bin',
    'loan_purpose_grouped',
    'loan_type',
    'applicant_derived_racial_category',
    'race_state_interaction',
    'distressed_or_underserved_race',
    'lender_registration_status'
]
df[categorical_cols] = df[categorical_cols].astype('category')
for col in categorical_cols:
    df[col] = df[col].cat.add_categories(['missing']).fillna('missing')

def race_flip(row, new_race): 
    row_cf = row.copy()
    old_race = row_cf['applicant_derived_racial_category']
    row_cf['applicant_derived_racial_category'] = new_race 
    row_cf['race_state_interaction'] = row_cf['race_state_interaction'].replace(old_race, new_race)
    row_cf['distressed_or_underserved_race'] = row_cf['distressed_or_underserved_race'].replace(old_race, new_race)

    return row_cf 

# Create table
create_table_sql = """
create or replace table hmda_redlining.source.race_counterfactual_results (
    loan_application_id varchar, 
    original_race varchar, 
    new_race varchar, 
    previous_score double, 
    counterfactual_score double, 
    score_change double
);
"""

with conn.cursor() as cur: 
    cur.execute(create_table_sql)

results = []
for i, row in df_model.iterrows(): 
    if i % 1000 == 1: 
        print(f"Currently on row {i}")
    for new_race in ['White', 'Black', 'Latinx', 'AAPI']: 
        original_idx = original_indices[i]
        original_race = df.loc[original_idx, "applicant_derived_racial_category"]
        if new_race == original_race: 
            continue 
        cf = race_flip(row, new_race)
        cf_score = model.predict_proba(pd.DataFrame([cf]))[:, 1][0]

        
        previous_score = df.loc[original_idx, "previous_score"]

        results.append({
            "loan_application_id": df.loc[original_idx, "loan_application_id"], 
            "original_race": original_race, 
            "new_race": new_race, 
            "previous_score": previous_score, 
            "counterfactual_score": cf_score, 
            "score_change": cf_score - previous_score 
        })

cf_df = pd.DataFrame(results)

print(f"Writing {len(cf_df)} rows to Snowflake")

success, _, nrows, _ = write_pandas(
    conn, 
    cf_df, 
    table_name='RACE_COUNTERFACTUAL_RESULTS', 
    schema='SOURCE', 
    database='HMDA_REDLINING', 
    quote_identifiers=False
)

if success:
    print(f"Inserted {nrows} into RACE_COUNTERFACTUAL_RESULTS")
else: 
    print(f"Failed to write the results")

conn.close()