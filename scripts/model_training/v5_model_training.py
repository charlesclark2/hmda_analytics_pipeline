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

sample_frac = 1.0
target = 'loan_approved'
chunk_size = 200_000
parquet_partitions = 4
n_trials = 10

def process_chunk(offset, limit, token, experiment_name):
    try:
        load_dotenv()
        con = duckdb.connect(f"motherduck:my_db?motherduck_token={token}")
        sql_chunk = f"""
        SELECT *
        FROM stg_balanced_class_sample
        LIMIT {limit} OFFSET {offset}
        """
        df_chunk = con.execute(sql_chunk).fetchdf()
        if df_chunk.empty:
            print(f"⚠️ Chunk {offset}-{offset+limit} skipped: empty chunk.")
            return f"Skipped chunk {offset}-{offset+limit}"

        ddf = dd.from_pandas(df_chunk, npartitions=parquet_partitions)
        exclude_cols = ['loan_application_id', 'census_tract', 'zip_code', 'county_code', 'state_code', target]
        cols_to_drop = [col for col in exclude_cols if col in ddf.columns]
        X = ddf.drop(columns=cols_to_drop)
        y = ddf[target]

        # Identify categorical columns
        categorical_cols = X.select_dtypes(include=['object', 'category']).columns.tolist()
        enforced_cats = [
            'lender_entity_name', 'lender_registration_status', 'applicant_derived_racial_category',
            'applicant_sex_category', 'applicant_age', 'loan_purpose_grouped', 'loan_type', 'occupancy_type',
            'race_sex_interaction', 'race_state_interaction', 'distressed_or_underserved_race',
            'dti_bin', 'minority_pct_bin', 'gini_bin', 'total_population_below_poverty_level_pct_log_bin',
            'avg_median_days_on_market_log_bin', 'strata_bin'
        ]
        for col in enforced_cats:
            if col in X.columns and col not in categorical_cols:
                categorical_cols.append(col)

        base_numerical_cols = X.select_dtypes(include=['number']).columns.tolist()
        all_columns = ddf.columns.tolist()
        numerical_cols = [col for col in all_columns if col not in categorical_cols + exclude_cols and col in base_numerical_cols]

        # Preprocessing for CatBoost (keep as before)
        preprocessor = ColumnTransformer(transformers=[
            ('num', Pipeline([
                ('imputer', SimpleImputer(strategy='median')),
                ('scaler', StandardScaler())
            ]), numerical_cols),
            ('cat', Pipeline([
                ('imputer', SimpleImputer(strategy='most_frequent')),
                ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
            ]), categorical_cols)
        ])

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=True)
        if y_train.compute().nunique() < 2:
            print(f"⚠️ Chunk {offset}-{offset+limit} skipped: only one class present.")
            return f"Skipped chunk {offset}-{offset+limit}"

        X_train_df = X_train.compute()
        X_test_df = X_test.compute()
        y_train_df = y_train.compute()
        y_test_df = y_test.compute()

        # Ensure category dtype for XGBoost
        for col in categorical_cols:
            if col in X_train_df.columns:
                X_train_df[col] = X_train_df[col].astype('category')
            if col in X_test_df.columns:
                X_test_df[col] = X_test_df[col].astype('category')

        models = ["CatBoostClassifier", "XGBoostClassifier"]
        logs = []

        for model_name in models:
            print(f"🔍 Tuning {model_name} on chunk {offset}-{offset+limit}")

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
                            'tree_method': 'hist',  # Required for categorical support
                            'enable_categorical': True  # Enable pandas category dtype handling
                        }
                        clf = xgb.XGBClassifier(**params)
                        clf.fit(X_train_df, y_train_df, eval_set=[(X_test_df, y_test_df)], verbose=False)
                    elif model_name == "CatBoostClassifier":
                        params = {
                            'iterations': trial.suggest_int('iterations', 100, 500),
                            'depth': trial.suggest_int('depth', 4, 10),
                            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                            'verbose': 0,
                            'loss_function': 'Logloss',
                            'task_type': 'CPU'
                        }
                        clf = CatBoostClassifier(**params)
                        clf.fit(X_train_df, y_train_df, cat_features=categorical_cols,
                                eval_set=(X_test_df, y_test_df), early_stopping_rounds=10)
                    else:
                        return 0

                    y_pred = clf.predict(X_test_df)
                    return roc_auc_score(y_test_df, y_pred)
                except Exception as e:
                    print(f"⚠️ Trial failed with error: {e}")
                    return 0

            study = optuna.create_study(direction='maximize')
            study.optimize(objective, n_trials=n_trials)
            best_params = study.best_params
            print(f"✅ Best params for {model_name}: {best_params}")

            # Fit best model
            if model_name == "CatBoostClassifier":
                model = CatBoostClassifier(**best_params)
                model.fit(X_train_df, y_train_df, cat_features=categorical_cols,
                          eval_set=(X_test_df, y_test_df), early_stopping_rounds=10)
            else:
                model = xgb.XGBClassifier(**{**best_params, 'tree_method': 'hist', 'enable_categorical': True})
                model.fit(X_train_df, y_train_df, eval_set=[(X_test_df, y_test_df)], verbose=False)

            # Evaluation
            y_pred = model.predict(X_test_df)
            auc = roc_auc_score(y_test_df, y_pred)
            acc = accuracy_score(y_test_df, y_pred)
            f1 = f1_score(y_test_df, y_pred)

            # Fairness metrics
            sensitive_features = X_test_df[['applicant_derived_racial_category', 'applicant_sex_category']]
            dpd = demographic_parity_difference(y_test_df, y_pred, sensitive_features=sensitive_features)
            eod = equalized_odds_difference(y_test_df, y_pred, sensitive_features=sensitive_features)

            # Feature Importances
            if hasattr(model, 'feature_importances_'):
                feature_importances = model.feature_importances_
                feature_names = X_train_df.columns
                fi_data = [(feature, float(importance), experiment_name, model_name, offset)
                           for feature, importance in zip(feature_names, feature_importances)]
                con.executemany("""
                    INSERT INTO feature_importance_log (feature, importance, experiment_name, model_name, chunk_offset)
                    VALUES (?, ?, ?, ?, ?)
                """, fi_data)

            # SHAP logging
            try:
                explainer = shap.TreeExplainer(model)
                shap_values = explainer.shap_values(X_test_df)
                mean_shap = pd.DataFrame(shap_values, columns=X_test_df.columns).abs().mean().reset_index()
                mean_shap.columns = ['feature', 'mean_shap_value']
                mean_shap['experiment_name'] = experiment_name
                mean_shap['model_name'] = model_name
                mean_shap['chunk_offset'] = offset
                con.executemany("""
                    INSERT INTO shap_values_log (feature, mean_shap_value, experiment_name, model_name, chunk_offset)
                    VALUES (?, ?, ?, ?, ?)
                """, mean_shap.values.tolist())
                print(f"✅ SHAP values logged for {model_name} on chunk {offset}-{offset+limit}")
            except Exception as e:
                print(f"❌ SHAP logging failed: {e}")

            logs.append((experiment_name, len(df_chunk), n_trials, model_name,
                         json.dumps(best_params), auc, acc, f1, dpd, eod, 1))

        con.executemany("""
            INSERT INTO model_training_logs (experiment_name, sample_size, number_of_training_trials, model_name,
                                             best_params, auc, accuracy, f1, dpd, eod, used_strata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, logs)

        return f"✅ Chunk {offset}-{offset+limit} processed successfully."

    except Exception as e:
        print(f"❌ Chunk {offset}-{offset+limit} failed with error: {e}")
        return f"❌ Chunk {offset}-{offset+limit} failed with error: {e}"




if __name__ == "__main__":
    cluster = LocalCluster(n_workers=2, threads_per_worker=2, memory_limit='6GB', dashboard_address=':8787')
    client = Client(cluster)
    client.run(gc.collect)
    print(client)

    token = os.getenv('MOTHERDUCK_TOKEN')
    experiment_name = f"experiment_{datetime.now().strftime('%Y%m%d_%H%M%s')}"
    print(f"Experiment name: {experiment_name}")
    total_rows = con.execute("SELECT count(*) FROM stg_balanced_class_sample").fetchone()[0]
    sample_rows = int(total_rows * sample_frac)
    print(f"Total rows: {total_rows}, Sampling {sample_frac*100:.1f}% => {sample_rows}")

    offsets = list(range(0, sample_rows, chunk_size))
    futures = [client.submit(process_chunk, offset, min(chunk_size, sample_rows - offset), token, experiment_name) for offset in offsets]
    for future in futures:
        print(future.result())
