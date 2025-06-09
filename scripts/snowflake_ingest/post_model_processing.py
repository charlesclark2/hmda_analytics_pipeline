import os
import pandas as pd
from catboost import CatBoostClassifier
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# === Load environment and model ===
load_dotenv()
sf_user = os.getenv("SNOWFLAKE_USER")
sf_password = os.getenv("SNOWFLAKE_PASSWORD")
sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
sf_database = "HMDA_REDLINING"
sf_schema = "SOURCE"
sf_role = os.getenv("SNOWFLAKE_ROLE")

model = CatBoostClassifier()
model.load_model("hmda_catboost_final_model.cbm")

# === Snowflake connection ===
conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema,
    role=sf_role
)

# === Create SCORED_RESULTS table if not exists ===
create_table_sql = f"""
CREATE OR REPLACE TABLE {sf_database}.{sf_schema}.SCORED_RESULTS (
    loan_application_id VARCHAR, 
    applicant_derived_racial_category VARCHAR, 
    applicant_age VARCHAR,
    actual_value INTEGER, 
    score DOUBLE, 
    predicted_label INTEGER
);
"""
with conn.cursor() as cur:
    cur.execute(create_table_sql)

# === Parameters ===
chunk_size = 500_000
start_offset = 1

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
sql_select_format_list = ', '.join(selected_cols)

# === Get total rows ===
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM HMDA_REDLINING.SOURCE.MART_HMDA_POST_MODEL_SET")
    total_rows = cur.fetchone()[0]
    print(f"📊 Total rows: {total_rows}")

# === Processing Loop ===
for offset in range(start_offset, total_rows, chunk_size):
    try:
        print(f"🚀 Processing offset {offset}")
        end = offset + chunk_size - 1

        sql = f"""
            SELECT loan_application_id, loan_approved, {sql_select_format_list}
            FROM HMDA_REDLINING.SOURCE.MART_HMDA_POST_MODEL_SET
            WHERE rn BETWEEN {offset} AND {end}
        """
        df_model = pd.read_sql_query(sql, conn)
        df_model.columns = df_model.columns.str.lower()  # Fix CatBoost name mismatch

        # Separate features
        df = df_model[selected_cols]
        # categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
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

        # Predict
        scores = model.predict_proba(df)[:, 1]
        preds = model.predict(df)

        result_df = pd.DataFrame({
            "loan_application_id": df_model["loan_application_id"],
            "applicant_derived_racial_category": df_model["applicant_derived_racial_category"],
            "applicant_age": df_model["applicant_age"],
            "actual_value": df_model["loan_approved"],
            "score": scores,
            "predicted_label": preds
        })

        # Upload to Snowflake
        success, _, nrows, _ = write_pandas(
            conn,
            result_df,
            table_name="SCORED_RESULTS",
            schema=sf_schema,
            database=sf_database,
            quote_identifiers=False
        )

        if success:
            print(f"✅ Inserted {nrows} rows into SCORED_RESULTS")
        else:
            print(f"❌ Failed to insert rows at offset {offset}")

    except Exception as e:
        print(f"❌ Processing failed at offset {offset}: {e}")
        continue

conn.close()
print("✅ All chunks processed and results saved.")
