import duckdb
import pandas as pd
from catboost import CatBoostClassifier
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, accuracy_score, f1_score
from fairlearn.metrics import demographic_parity_difference, equalized_odds_difference
from dotenv import load_dotenv
import os 
from sklearn.model_selection import train_test_split
import shap 
import json 

experiment_name = f"Final_experiment_validation_full_set_dedupe"
load_dotenv()
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")

selected_cols = ['activity_year', 'loan_approved', 'dti_bin', 'lender_prior_year_approval_rate', 
                     'loan_purpose_grouped', 'property_value', 'lender_prior_approval_rate_white', 
                     'loan_type', 'loan_amount', 'lender_prior_approval_rate_black', 
                     'lender_prior_approval_rate_latinx', 'lender_prior_approval_rate_aapi', 
                     'lender_minority_gap', 'income_log_x_property_value', 'prior_approval_rate', 
                     'income_to_loan_ratio_stratified', 'applicant_age', 
                     'income_log', 'income', 'applicant_derived_racial_category', 
                     'race_state_interaction', 'distressed_or_underserved_race', 
                     'avg_median_price_per_square_foot', 'pct_bachelors_or_higher', 
                     'loan_to_income_ratio', 'lender_registration_status', 'minority_population_pct', 
                     'gini_index_of_income_inequality', 'tract_minority_gap', 'gini_x_income_log']

sql_select_format_list = ', '.join(selected_cols)
sql_select_format_list = f"{sql_select_format_list} "
sql_text = f"""
select 
    {sql_select_format_list}
from mart_hmda_validation_set
"""

print(f"🔍 Loading data ")
df_chunk = con.execute(sql_text).fetchdf()

target = 'loan_approved'
sensitive_features = ['applicant_derived_racial_category']

df_chunk = df_chunk[selected_cols]
X_val = df_chunk.drop(columns=[target])
y_val = df_chunk['loan_approved']

# Identify categorical columns
categorical_cols = X_val.select_dtypes(include=['object', 'category']).columns.tolist()
X_val[categorical_cols] = X_val[categorical_cols].astype('category')

y_val_df = y_val.reset_index(drop=True)

params = {
    "iterations": 345, 
    "depth": 10, 
    "learning_rate": 0.05923384341818935, 
    "bootstrap_type": "Bernoulli", 
    "subsample": 0.6877181010636558, 
    "eval_metric": "AUC", 
    "early_stopping_rounds": 50, 
    "verbose": 20, 
    "random_seed": 42, 
    "used_ram_limit": "8gb"
}

# Prepare CatBoost-friendly data
X_catboost = X_val.copy()
for col in categorical_cols:
    X_catboost[col] = X_catboost[col].cat.add_categories(['missing']).fillna('missing')

model = CatBoostClassifier()
model.load_model("hmda_catboost_final_model.cbm")

y_pred = model.predict(X_catboost)
y_proba = model.predict_proba(X_catboost)[:, 1]

sensitive_features_df = X_val[sensitive_features].reset_index(drop=True)

auc_prob = roc_auc_score(y_val, y_proba)
auc_pred = roc_auc_score(y_val, y_pred)
f1 = f1_score(y_val, y_pred)
acc = accuracy_score(y_val, y_pred)
print("AUC Prob: ", auc_prob)
print("AUC Pred: ", auc_pred)
print(f"F1 Score: {f1}")
print(f"Accuracy: {acc}")

try:
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_val)
    mean_shap = pd.DataFrame(shap_values, columns=X_val.columns).abs().mean().reset_index()
    mean_shap.columns = ['feature', 'mean_shap_value']
    mean_shap['experiment_name'] = experiment_name
    mean_shap['model_name'] = 'CatBoostClassifer'
    mean_shap['chunk_offset'] = 0
    con.executemany("""
        INSERT INTO shap_values_log (feature, mean_shap_value, experiment_name, model_name, chunk_offset)
        VALUES (?, ?, ?, ?, ?)
    """, mean_shap.values.tolist())
    print(f"✅ SHAP values logged for 'CatBoostClassifier'on ")
except Exception as e:
    print(f"❌ SHAP logging failed: {e}")


dpd = demographic_parity_difference(y_val_df, y_pred, sensitive_features=sensitive_features_df)
eod = equalized_odds_difference(y_val_df, y_pred, sensitive_features=sensitive_features_df)
print(f"DPD: {dpd:.4f}, EOD: {eod:.4f}")

print(f"Logging values")
con.execute("""
        INSERT INTO model_training_logs (experiment_name, sample_size, number_of_training_trials, model_name,
                                            best_params, auc, accuracy, f1, dpd, eod, used_strata)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (experiment_name, len(df_chunk), 0, 'CatBoostClassifier', json.dumps(params), auc_pred, acc, f1, dpd, eod, 1))

print("Complete")