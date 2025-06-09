import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import pandas as pd
from catboost import CatBoostClassifier, Pool
import xgboost as xgb
from sklearn.metrics import roc_auc_score, accuracy_score, f1_score
import optuna
from fairlearn.metrics import demographic_parity_difference, equalized_odds_difference
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from dask_ml.model_selection import train_test_split
from dotenv import load_dotenv
import os, json, time, duckdb
from datetime import datetime
import warnings
import shap
import gc

warnings.filterwarnings("ignore")

# Setup environment
load_dotenv()
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
print("Connecting to MotherDuck...")
con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")

target = 'loan_approved'
chunk_size = 600_000
chunks_needed = 600_000 // chunk_size
parquet_partitions = 4
n_trials = 30

import threading

def compute_shap_with_timeout(model, X_test, timeout_sec=180):
    result = {'shap_df': None, 'error': None}
    def target():
        try:
            explainer = shap.TreeExplainer(model)
            shap_vals = explainer.shap_values(X_test)
            shap_df = pd.DataFrame(shap_vals, columns=X_test.columns).abs().mean().reset_index()
            shap_df.columns = ['feature', 'mean_shap_value']
            result['shap_df'] = shap_df
        except Exception as e:
            result['error'] = str(e)
    
    thread = threading.Thread(target=target)
    thread.start()
    thread.join(timeout=timeout_sec)
    if thread.is_alive():
        print(f"⏰ SHAP computation timed out after {timeout_sec} seconds.")
        return None
    if result['error']:
        print(f"❌ SHAP computation failed: {result['error']}")
        return None
    return result['shap_df']


def process_chunk(chunk_number, token, experiment_name):
    import duckdb, pandas as pd, shap, json
    from catboost import CatBoostClassifier
    import xgboost as xgb
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import roc_auc_score, accuracy_score, f1_score
    from fairlearn.metrics import demographic_parity_difference, equalized_odds_difference
    import optuna
    from optuna.integration import CatBoostPruningCallback
    import optuna.visualization as vis 

    experiment_name = f"{experiment_name}_chunk_{chunk_number}"
    con = duckdb.connect(f"motherduck:my_db?motherduck_token={token}")

    print(f"🔍 Loading chunk {chunk_number}")
    df_chunk = con.execute(f"""
        SELECT * FROM stg_alpha_model_training WHERE chunk_number = {chunk_number} 
    """).fetchdf()

    if df_chunk.empty:
        print(f"⚠️ Chunk {chunk_number} skipped: empty")
        return

    target = 'loan_approved'
    sensitive_features = ['applicant_derived_racial_category']
    # exclude_cols = ['global_row_number', 'chunk_number']
    # test_remove_features = ['minority_pct_bin', 'occupancy_type', 'applicant_sex_category', 
    #                         'tract_prior_approval_rate_latinx', 'tract_prior_approval_rate_aapi', 
    #                         'tract_prior_approval_rate_black', 'tract_prior_approval_rate_white']
    # Adding for the validation run with selected features
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
    df_chunk = df_chunk[selected_cols]
    X = df_chunk.drop(columns=[target])
    y = df_chunk[target]

    # Identify categorical columns
    categorical_cols = X.select_dtypes(include=['object', 'category']).columns.tolist()
    X[categorical_cols] = X[categorical_cols].astype('category')

    # Prepare CatBoost-friendly data
    X_catboost = X.copy()
    for col in categorical_cols:
        X_catboost[col] = X_catboost[col].cat.add_categories(['missing']).fillna('missing')

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(X_catboost, y, test_size=0.2, random_state=42, shuffle=True)
    sensitive_features_df = X_test[sensitive_features].reset_index(drop=True)
    y_test_df = y_test.reset_index(drop=True)

    model_name = 'CatBoostClassifier'
    print(f"🔍 Starting {model_name} on chunk {chunk_number} with baseline parameters")
    
    # Now move to Optuna tuning
    print(f"🔍 Starting Optuna tuning for {model_name} on chunk {chunk_number}")
    def objective(trial):
        try:
            ### final params testing
            params = {
                "iterations": trial.suggest_int("iterations", 250, 350),
                "depth": trial.suggest_int("depth", 8, 11),
                "learning_rate": trial.suggest_float("learning_rate", 0.03, 0.08),
                "bootstrap_type": trial.suggest_categorical("bootstrap_type", ["Bayesian"]),
                "bagging_temperature": trial.suggest_float("bagging_temperature", 0.5, 3.0),
                "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 5.0),
                "random_strength": trial.suggest_float("random_strength", 1.0, 10.0),
                "eval_metric": "AUC",
                "loss_function": "Logloss",
                "used_ram_limit": "6gb",
                "verbose": 0
            }

            ### second iteration params
            # params = {
            #     'iterations': trial.suggest_int('iterations', 300, 500),
            #     'depth': trial.suggest_int('depth', 8, 12),
            #     'learning_rate': trial.suggest_float('learning_rate', 0.06, 0.1),
            #     'l2_leaf_reg': trial.suggest_float('l2_leaf_reg', 1.0, 10.0), 
            #     'random_strength': trial.suggest_float('random_strength', 1.0, 20.0), 
            #     'bagging_temperature': trial.suggest_float('bagging_temperature', 0.0, 1.0),
            #     'verbose': 0,
            #     'loss_function': 'Logloss', 
            #     'eval_metric': "AUC", 
            #     "used_ram_limit": "8gb"
            # }
            ### first iteration params
            # params = {
            #     "iterations": trial.suggest_int("iterations", 150, 400),
            #     "depth": trial.suggest_int("depth", 6, 10),
            #     "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.08),
            #     "bootstrap_type": trial.suggest_categorical("bootstrap_type", ["Bayesian", "Bernoulli"]),
            #     "eval_metric": "AUC",
            #     "loss_function": "Logloss",
            #     "used_ram_limit": "6gb",
            #     "verbose": 0
            # }
            # if params["bootstrap_type"] == "Bayesian":
            #     params["bagging_temperature"] = trial.suggest_float("bagging_temperature", 0, 5)
            # elif params["bootstrap_type"] == "Bernoulli":
            #     params["subsample"] = trial.suggest_float("subsample", 0.5, 1, log=True)
            model = CatBoostClassifier(**params)
            pruning_callback = CatBoostPruningCallback(trial, "AUC")
            model.fit(
                X_train, 
                y_train, 
                cat_features=categorical_cols, 
                eval_set=(X_test, y_test), 
                early_stopping_rounds=20, 
                callbacks=[pruning_callback]
            )
            preds = model.predict(X_test)
            auc = roc_auc_score(y_test, preds)
            acc = accuracy_score(y_test, preds)
            f1 = f1_score(y_test, preds)
            dpd = demographic_parity_difference(y_test_df, preds, sensitive_features=sensitive_features_df)
            eod = equalized_odds_difference(y_test_df, preds, sensitive_features=sensitive_features_df)
            print(f"📈 Trial AUC for {model_name}: {auc:.4f}")
            print(f"Other metrics: acc: {acc:.4f}, f1: {f1:.4f}, dpd: {dpd:.4f}, eod: {eod:.4f}")
            return auc
        except Exception as e:
            print(f"⚠️ Trial error for {model_name}: {e}")
            return 0

    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials)
    best_params = study.best_params
    print(f"✅ Best Optuna params for {model_name}: {best_params}")

    # Final Optuna model training

    model = CatBoostClassifier(**best_params)
    model.fit(X_train, y_train, cat_features=categorical_cols, eval_set=(X_test, y_test), early_stopping_rounds=10)

    preds = model.predict(X_test)
    auc = roc_auc_score(y_test, preds)
    acc = accuracy_score(y_test, preds)
    f1 = f1_score(y_test, preds)
    dpd = demographic_parity_difference(y_test_df, preds, sensitive_features=sensitive_features_df)
    eod = equalized_odds_difference(y_test_df, preds, sensitive_features=sensitive_features_df)
    print(f"📊 {model_name} Final Optuna AUC: {auc:.4f}")
    print(f"Other metrics: acc: {acc:.4f}, f1: {f1:.4f}, dpd: {dpd:.4f}, eod: {eod:.4f}")

    print(f"Saving visualizations")
    fig = vis.plot_optimization_history(study)
    optimization_file_name = f"./model_run_images/{experiment_name}_optimization_history.html"
    fig.write_html(optimization_file_name)

    fig = vis.plot_param_importances(study)
    param_import_file_name = f"./model_run_images/{experiment_name}_param_importances.html"
    fig.write_html(param_import_file_name)

    fig = vis.plot_contour(study)
    contour_file_name = f"./model_run_images/{experiment_name}_contour.html"
    fig.write_html(contour_file_name)

    print(f"All images created")

    # Feature importances and SHAP for baseline
    if hasattr(model, 'feature_importances_'):
        fi_data = [(f, float(i), experiment_name+"_baseline", model_name, chunk_number)
                    for f, i in zip(X.columns, model.feature_importances_)]
        con.executemany("""
            INSERT INTO feature_importance_log (feature, importance, experiment_name, model_name, chunk_offset)
            VALUES (?, ?, ?, ?, ?)
        """, fi_data)
        print(f"✅ Feature importances logged for {model_name} baseline")

    print(f"🔍 Attempting SHAP for {model_name} baseline")
    # SHAP logging 

    try:
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test)
        mean_shap = pd.DataFrame(shap_values, columns=X_test.columns).abs().mean().reset_index()
        mean_shap.columns = ['feature', 'mean_shap_value']
        mean_shap['experiment_name'] = experiment_name
        mean_shap['model_name'] = model_name
        mean_shap['chunk_offset'] = chunk_number
        con.executemany("""
            INSERT INTO shap_values_log (feature, mean_shap_value, experiment_name, model_name, chunk_offset)
            VALUES (?, ?, ?, ?, ?)
        """, mean_shap.values.tolist())
        print(f"✅ SHAP values logged for {model_name} on chunk {chunk_number}")
    except Exception as e:
        print(f"❌ SHAP logging failed: {e}")

    con.execute("""
        INSERT INTO model_training_logs (experiment_name, sample_size, number_of_training_trials, model_name,
                                            best_params, auc, accuracy, f1, dpd, eod, used_strata)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (experiment_name, len(df_chunk), n_trials, model_name, json.dumps(best_params), auc, acc, f1, dpd, eod, 1))
    print(f"📦 Optuna metrics logged for {model_name}")

    return f"✅ Chunk {chunk_number} complete"


if __name__ == "__main__":
    from dask.distributed import Client, LocalCluster
    from datetime import datetime
    import os
    import gc
    import duckdb

    # Dask cluster setup (optimize for 12GB total disk space)
    cluster = LocalCluster(
        n_workers=1,  # reduce workers to limit disk I/O pressure
        threads_per_worker=1,
        memory_limit='10GB',  # per worker memory cap
        dashboard_address=':8787'
    )
    client = Client(cluster)
    client.run(gc.collect)
    print(client)

    # Load token and setup experiment name
    token = os.getenv('MOTHERDUCK_TOKEN')
    experiment_base = f"experiment_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"Experiment base: {experiment_base}")

    # Connect to DuckDB and determine total rows & chunks
    con = duckdb.connect(f"motherduck:my_db?motherduck_token={token}")
    chunk_size = 400_000  # new chunk size
    total_rows = con.execute("SELECT COUNT(*) FROM stg_alpha_model_training where chunk_number = 0").fetchone()[0]
    # chunks_needed = (total_rows + chunk_size - 1) // chunk_size  # ceiling division
    chunk_start = 7
    chunk_end = 10
    print(f"Total rows: {total_rows}, Chunk size: {chunk_size}, Chunks needed: {chunks_needed}")

    for chunk_number in range(chunk_start, chunk_end): 
        experiment_name = f"{experiment_base}_chunk_{chunk_number}"
        process_chunk(chunk_number, token, experiment_name)

    # Submit Dask tasks per chunk_number
    # futures = []
    # for chunk_number in range(chunk_start, chunk_end):
    #     experiment_name = f"{experiment_base}_chunk_{chunk_number}"
    #     futures.append(client.submit(process_chunk, chunk_number, token, experiment_name))  # 10 trials for speed

    # # Gather and print results
    # for future in futures:
    #     print(future.result())
