import duckdb
import pandas as pd
from catboost import CatBoostClassifier
from dotenv import load_dotenv
import os
from datetime import datetime

# === Load environment and model ===
load_dotenv()
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
model = CatBoostClassifier()
model.load_model("hmda_catboost_final_model.cbm")

# === Config ===
parquet_dir = "/Volumes/T9/hmda_project/parquet_chunks"
os.makedirs(parquet_dir, exist_ok=True)

db_path = "/Volumes/T9/hmda_project/post_model_scores.duckdb"
local_con = duckdb.connect(db_path)

# Create scored_results table if not exists
local_con.execute("""
    CREATE TABLE IF NOT EXISTS scored_results (
        loan_application_id VARCHAR,
        actual_value INTEGER,
        score DOUBLE,
        predicted_label INTEGER
    );
""")

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

# === Connect to MotherDuck ===
con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")

# === Row count check ===
total_rows = con.execute("SELECT COUNT(*) FROM mart_hmda_preprocessed").fetchone()[0]
print(f"📊 Total rows: {total_rows}")

# === Processing Loop ===
for offset in range(start_offset, total_rows, chunk_size):
    if offset == 27500001:
        continue  # skip known bad chunk

    try:
        print(f"🚀 Processing offset {offset}")
        end = offset + chunk_size - 1

        # Step 1: Export mart_hmda_preprocessed chunk to Parquet
        df_export = con.execute(f"""
            SELECT * FROM mart_hmda_preprocessed
            WHERE rn BETWEEN {offset} AND {end}
        """).fetchdf()

        parquet_path = os.path.join(parquet_dir, f"mart_hmda_preprocessed_{offset}_{end}.parquet")
        df_export.to_parquet(parquet_path, index=False)
        print(f"📦 Exported {len(df_export)} rows to {parquet_path}")

        # Step 2: Fetch model input and labels
        df_model = con.execute(f"""
            SELECT loan_application_id, loan_approved, {sql_select_format_list}
            FROM mart_hmda_post_model_set
            WHERE rn BETWEEN {offset} AND {end}
        """).fetchdf()

        df = df_model[selected_cols]
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        df[categorical_cols] = df[categorical_cols].astype('category')
        for col in categorical_cols:
            df[col] = df[col].cat.add_categories(['missing']).fillna('missing')

        scores = model.predict_proba(df[selected_cols])[:, 1]
        preds = model.predict(df[selected_cols])

        result_df = pd.DataFrame({
            "loan_application_id": df_model["loan_application_id"],
            "actual_value": df_model["loan_approved"],
            "score": scores,
            "predicted_label": preds
        })

        # Step 3: Insert scoring results into DuckDB
        local_con.register("temp_results", result_df)
        local_con.execute("""
            INSERT INTO scored_results
            SELECT * FROM temp_results
            WHERE loan_application_id NOT IN (
                SELECT loan_application_id FROM scored_results
            )
        """)
        local_con.unregister("temp_results")

        count = local_con.execute("SELECT COUNT(*) FROM scored_results").fetchone()[0]
        print(f"✅ Total rows in scored_results: {count}")

    except Exception as e:
        print(f"❌ Failed at offset {offset}: {e}")
        continue

local_con.close()
print("✅ All chunks processed and results saved.")
