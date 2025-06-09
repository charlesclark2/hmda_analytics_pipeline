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
chunk_size = 400_000
chunks_needed = 10_000_000 // chunk_size
parquet_partitions = 4
n_trials = 30


def process_chunk(chunk_number, token, experiment_name):
    experiment_name = f"{experiment_name}_chunk_{chunk_number}"
    try:
        load_dotenv()
        con = duckdb.connect(f"motherduck:my_db?motherduck_token={token}")

        sql_chunk = f"""
        SELECT *
        FROM stg_training_sample_stratified
        WHERE chunk_number = {chunk_number}
        """
        df_chunk = con.execute(sql_chunk).fetchdf()
        if df_chunk.empty:
            print(f"⚠️ Chunk {chunk_number} skipped: empty chunk.")
            return f"Skipped chunk {chunk_number}"

        target = 'loan_approved'
        sensitive_features = ['applicant_derived_racial_category', 'applicant_sex_category']
        exclude_cols = ['global_row_number', 'chunk_number', 'lender_entity_name']
        cols_to_drop = [col for col in exclude_cols if col in df_chunk.columns]
        X = df_chunk.drop(columns=cols_to_drop + [target])
        y = df_chunk[target]

        # Identify categorical and numeric columns
        categorical_cols = X.select_dtypes(include=['object', 'category']).columns.tolist()
        enforced_cols = [
            'loan_purpose_grouped', 'dti_bin', 'applicant_age', 'race_state_interaction',
            'applicant_derived_racial_category', 'lender_registration_status',
            'distressed_or_underserved_race', 'race_sex_interaction',
            'minority_pct_bin', 'applicant_sex_category'
        ]
        for col in enforced_cols:
            if col in X.columns and col not in categorical_cols:
                categorical_cols.append(col)
        numerical_cols = [col for col in X.columns if col not in categorical_cols and col not in sensitive_features]

        # Ensure categorical columns are converted properly for both models
        for col in categorical_cols:
            if col in X.columns:
                X[col] = X[col].fillna('missing').astype('category')

        from sklearn.model_selection import train_test_split
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=True)
        sensitive_features_df = X_test[sensitive_features].reset_index(drop=True)
        y_test_df = y_test.reset_index(drop=True)

        models = ["CatBoostClassifier", "XGBoostClassifier"]
        logs = []

        for model_name in models:
            print(f"🔍 Tuning {model_name} on chunk {chunk_number}")

            def objective(trial):
                try:
                    if model_name == "XGBoostClassifier":
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
                        clf = xgb.XGBClassifier(**params)
                        clf.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
                    else:
                        params = {
                            'iterations': trial.suggest_int('iterations', 100, 500),
                            'depth': trial.suggest_int('depth', 4, 10),
                            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                            'verbose': 0,
                            'loss_function': 'Logloss',
                            'task_type': 'CPU'
                        }
                        clf = CatBoostClassifier(**params)
                        clf.fit(X_train, y_train, cat_features=categorical_cols,
                                eval_set=(X_test, y_test), early_stopping_rounds=10)
                    y_pred = clf.predict(X_test)
                    return roc_auc_score(y_test_df, y_pred)
                except Exception as e:
                    print(f"⚠️ Trial failed with error: {e}")
                    return 0

            study = optuna.create_study(direction='maximize')
            study.optimize(objective, n_trials=n_trials)
            best_params = study.best_params
            print(f"✅ Best params for {model_name}: {best_params}")

            # Final model training
            if model_name == "XGBoostClassifier":
                model = xgb.XGBClassifier(**{**best_params, 'tree_method': 'hist', 'enable_categorical': True})
                model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
                y_pred = model.predict(X_test)
                feature_cols = X_train.columns
            else:
                model = CatBoostClassifier(**best_params)
                model.fit(X_train, y_train, cat_features=categorical_cols,
                          eval_set=(X_test, y_test), early_stopping_rounds=10)
                y_pred = model.predict(X_test)
                feature_cols = X_train.columns

            print(f"Scoring {model_name} for chunk {chunk_number}")
            auc = roc_auc_score(y_test_df, y_pred)
            acc = accuracy_score(y_test_df, y_pred)
            f1 = f1_score(y_test_df, y_pred)

            dpd = demographic_parity_difference(y_test_df, y_pred, sensitive_features=sensitive_features_df)
            eod = equalized_odds_difference(y_test_df, y_pred, sensitive_features=sensitive_features_df)
            print(f"Current fairness metrics, dpd: {dpd}, eod: {eod}, model: {model_name}, chunk: {chunk_number}")

            print(f"feature importances {model_name} for chunk {chunk_number}")
            # Feature Importances
            if hasattr(model, 'feature_importances_'):
                feature_importances = model.feature_importances_
                fi_data = [(feature, float(importance), experiment_name, model_name, chunk_number)
                           for feature, importance in zip(feature_cols, feature_importances)]
                con.executemany("""
                    INSERT INTO feature_importance_log (feature, importance, experiment_name, model_name, chunk_offset)
                    VALUES (?, ?, ?, ?, ?)
                """, fi_data)

            print(f"Shap {model_name} for chunk {chunk_number}")
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

            logs.append((experiment_name, len(df_chunk), 1, model_name, json.dumps(best_params),
                         auc, acc, f1, dpd, eod, 1))

        con.executemany("""
            INSERT INTO model_training_logs (experiment_name, sample_size, number_of_training_trials, model_name,
                                             best_params, auc, accuracy, f1, dpd, eod, used_strata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, logs)

        return f"✅ Chunk {chunk_number} processed successfully."

    except Exception as e:
        print(f"❌ Chunk {chunk_number} failed with error: {e}")
        return f"❌ Chunk {chunk_number} failed with error: {e}"



if __name__ == "__main__":
    from dask.distributed import Client, LocalCluster
    from datetime import datetime
    import os
    import gc

    # Dask cluster setup
    cluster = LocalCluster(n_workers=3, threads_per_worker=3, memory_limit='4GB', dashboard_address=':8787')
    client = Client(cluster)
    client.run(gc.collect)
    print(client)

    token = os.getenv('MOTHERDUCK_TOKEN')
    experiment_base = f"experiment_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"Experiment base: {experiment_base}")

    # Optional: query total rows and rows per chunk (informational)
    import duckdb
    con = duckdb.connect(f"motherduck:my_db?motherduck_token={token}")
    total_rows = con.execute(f"SELECT count(*) FROM stg_training_sample_stratified WHERE chunk_number < {chunks_needed}").fetchone()[0]
    print(f"Total rows in selected range (~10M rows): {total_rows}")

    # Submit Dask tasks per chunk_number
    futures = []
    for chunk_number in range(chunks_needed):
        experiment_name = f"{experiment_base}_chunk_{chunk_number}"
        futures.append(client.submit(process_chunk, chunk_number, token, experiment_name))

    # Gather and print results
    for future in futures:
        print(future.result())
