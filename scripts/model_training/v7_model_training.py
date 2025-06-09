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
n_trials = 15

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

    experiment_name = f"{experiment_name}_chunk_{chunk_number}"
    con = duckdb.connect(f"motherduck:my_db?motherduck_token={token}")

    print(f"🔍 Loading chunk {chunk_number}")
    df_chunk = con.execute(f"""
        SELECT * FROM stg_real_class_balance_by_year WHERE chunk_number = {chunk_number} order by random() limit 400000
    """).fetchdf()

    if df_chunk.empty:
        print(f"⚠️ Chunk {chunk_number} skipped: empty")
        return

    target = 'loan_approved'
    sensitive_features = ['applicant_derived_racial_category', 'applicant_sex_category']
    exclude_cols = ['global_row_number', 'chunk_number']
    X = df_chunk.drop(columns=exclude_cols + [target])
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

    models = ['CatBoostClassifier', 'XGBoostClassifier']
    # models = ['XGBoostClassifier']
    logs = []

    # Baseline hyperparameters (as you provided earlier)
    baseline_params = {
        'CatBoostClassifier': {'iterations': 296, 'depth': 8, 'learning_rate': 0.2783715704866244, 'verbose': 0, 'loss_function': 'Logloss'},
        'XGBoostClassifier': {'n_estimators': 290, 'max_depth': 9, 'learning_rate': 0.09941564961706434,
                              'subsample': 0.8856880241827817, 'colsample_bytree': 0.719899920299138,
                              'use_label_encoder': False, 'eval_metric': 'logloss', 'tree_method': 'hist', 'enable_categorical': True}
    }

    for model_name in models:
        print(f"🔍 Starting {model_name} on chunk {chunk_number} with baseline parameters")
        if model_name == 'CatBoostClassifier':
            model = CatBoostClassifier(**baseline_params[model_name])
            model.fit(X_train, y_train, cat_features=categorical_cols, eval_set=(X_test, y_test), early_stopping_rounds=10)
        else:
            model = xgb.XGBClassifier(**baseline_params[model_name])
            model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
        
        preds = model.predict(X_test)
        auc = roc_auc_score(y_test, preds)
        acc = accuracy_score(y_test, preds)
        f1 = f1_score(y_test, preds)
        dpd = demographic_parity_difference(y_test_df, preds, sensitive_features=sensitive_features_df)
        eod = equalized_odds_difference(y_test_df, preds, sensitive_features=sensitive_features_df)
        print(f"📈 {model_name} baseline AUC: {auc:.4f}")

        # Logging for baseline
        con.execute("""
            INSERT INTO model_training_logs (experiment_name, sample_size, number_of_training_trials, model_name,
                                             best_params, auc, accuracy, f1, dpd, eod, used_strata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (experiment_name+"_baseline", len(df_chunk), 0, model_name, json.dumps(baseline_params[model_name]), auc, acc, f1, dpd, eod, 1))
        print(f"📦 Baseline metrics logged for {model_name}")

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
        # SHAP logging (CatBoost only)
        if model_name == "CatBoostClassifier":
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
        # shap_df = compute_shap_with_timeout(model, X_test)
        # if shap_df is not None:
        #     shap_df['experiment_name'], shap_df['model_name'], shap_df['chunk_offset'] = experiment_name+"_baseline", model_name, chunk_number
        #     con.executemany("""
        #         INSERT INTO shap_values_log (feature, mean_shap_value, experiment_name, model_name, chunk_offset)
        #         VALUES (?, ?, ?, ?, ?)
        #     """, shap_df.values.tolist())
        #     print(f"📦 SHAP logged")
        # else:
        #     print(f"⚠️ SHAP skipped for {model_name}")


        # Now move to Optuna tuning
        print(f"🔍 Starting Optuna tuning for {model_name} on chunk {chunk_number}")
        def objective(trial):
            try:
                if model_name == 'CatBoostClassifier':
                    params = {
                        'iterations': trial.suggest_int('iterations', 100, 500),
                        'depth': trial.suggest_int('depth', 4, 10),
                        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                        'verbose': 0,
                        'loss_function': 'Logloss'
                    }
                    model = CatBoostClassifier(**params)
                    model.fit(X_train, y_train, cat_features=categorical_cols, eval_set=(X_test, y_test), early_stopping_rounds=10)
                else:
                    params = {
                        'n_estimators': trial.suggest_int('n_estimators', 100, 300),
                        'max_depth': trial.suggest_int('max_depth', 3, 10),
                        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
                        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
                        'use_label_encoder': False,
                        'eval_metric': 'logloss',
                        'tree_method': 'hist',
                        'enable_categorical': True
                    }
                    model = xgb.XGBClassifier(**params)
                    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
                preds = model.predict(X_test)
                auc = roc_auc_score(y_test, preds)
                print(f"📈 Trial AUC for {model_name}: {auc:.4f}")
                return auc
            except Exception as e:
                print(f"⚠️ Trial error for {model_name}: {e}")
                return 0

        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials)
        best_params = study.best_params
        print(f"✅ Best Optuna params for {model_name}: {best_params}")

        # Final Optuna model training
        if model_name == 'CatBoostClassifier':
            model = CatBoostClassifier(**best_params)
            model.fit(X_train, y_train, cat_features=categorical_cols, eval_set=(X_test, y_test), early_stopping_rounds=10)
        else:
            model = xgb.XGBClassifier(**{**best_params, 'tree_method': 'hist', 'enable_categorical': True})
            model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        preds = model.predict(X_test)
        auc = roc_auc_score(y_test, preds)
        acc = accuracy_score(y_test, preds)
        f1 = f1_score(y_test, preds)
        dpd = demographic_parity_difference(y_test_df, preds, sensitive_features=sensitive_features_df)
        eod = equalized_odds_difference(y_test_df, preds, sensitive_features=sensitive_features_df)
        print(f"📊 {model_name} Final Optuna AUC: {auc:.4f}")

        con.execute("""
            INSERT INTO model_training_logs (experiment_name, sample_size, number_of_training_trials, model_name,
                                             best_params, auc, accuracy, f1, dpd, eod, used_strata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (experiment_name, len(df_chunk), 10, model_name, json.dumps(best_params), auc, acc, f1, dpd, eod, 1))
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
    total_rows = con.execute("SELECT COUNT(*) FROM stg_real_class_balance_by_year where chunk_number = 0").fetchone()[0]
    # chunks_needed = (total_rows + chunk_size - 1) // chunk_size  # ceiling division
    chunk_start = 3
    chunk_end = 5
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
