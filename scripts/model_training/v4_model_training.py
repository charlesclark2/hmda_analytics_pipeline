import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from catboost import CatBoostClassifier
import xgboost as xgb
from sklearn.metrics import roc_auc_score, accuracy_score, f1_score
import optuna
from fairlearn.metrics import MetricFrame, selection_rate, demographic_parity_difference, equalized_odds_difference
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import cross_val_score
from dotenv import load_dotenv
from dask_ml.model_selection import train_test_split
import os, json, time, duckdb, warnings, shap, gc
from datetime import datetime

warnings.filterwarnings("ignore")
load_dotenv()

motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")

sample_frac = 1.0
target = 'loan_approved'
chunk_size = 200_000
parquet_partitions = 4
n_trials = 10

def process_chunk(offset, limit, token, experiment_name):
    try:
        load_dotenv()
        import duckdb
        import gc

        con = duckdb.connect(f"motherduck:my_db?motherduck_token={token}")
        sql_chunk = f"""
        SELECT *
        FROM stg_hmda_training_sampled
        WHERE strata_bin IS NOT NULL
        LIMIT {limit} OFFSET {offset}
        """
        df_chunk = con.execute(sql_chunk).fetchdf()
        if df_chunk.empty:
            return f"⚠️ Chunk {offset}-{offset+limit} skipped: empty chunk."
        if 'lender_entity_name' in df_chunk.columns:
            df_chunk['lender_entity_name'] = df_chunk['lender_entity_name'].astype('category')

        ddf = dd.from_pandas(df_chunk, npartitions=parquet_partitions)

        exclude_cols = ['loan_application_id', 'census_tract', 'zip_code', 'county_code', 'state_code', target]
        X = ddf.drop(columns=exclude_cols)
        y = ddf[target]
        categorical_cols = X.select_dtypes(include=['object', 'category']).columns.tolist()
        base_numerical_cols = X.select_dtypes(include=['number']).columns.tolist()
        all_columns = ddf.columns.tolist()

        if 'lender_entity_name' not in categorical_cols:
            categorical_cols.append('lender_entity_name')

        numerical_cols = [col for col in all_columns if col not in categorical_cols + exclude_cols and col in base_numerical_cols]

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
            return f"⚠️ Chunk {offset}-{offset+limit} skipped: only one class present."

        models = {
            "CatBoostClassifier": CatBoostClassifier(verbose=0),
            "XGBoostClassifier": xgb.XGBClassifier(use_label_encoder=False, eval_metric='logloss')
        }

        logs = []
        for model_name, model in models.items():
            print(f"Training {model_name} on chunk {offset}-{offset+limit}")
            pipeline = Pipeline([('preprocessor', preprocessor), ('model', model)])
            pipeline.fit(X_train.compute(), y_train.compute())

            y_pred = pipeline.predict(X_test.compute())
            auc = roc_auc_score(y_test.compute(), y_pred)
            acc = accuracy_score(y_test.compute(), y_pred)
            f1 = f1_score(y_test.compute(), y_pred)
            sensitive_cols = ['applicant_derived_racial_category', 'applicant_sex_category', 'applicant_age']
            sensitive_feature = X_test.compute()[sensitive_cols]
            dpd = demographic_parity_difference(y_test.compute(), y_pred, sensitive_features=sensitive_feature)
            eod = equalized_odds_difference(y_test.compute(), y_pred, sensitive_features=sensitive_feature)

            model_fitted = pipeline.named_steps['model']
            if hasattr(model_fitted, 'feature_importances_'):
                try:
                    feature_names = pipeline.named_steps['preprocessor'].get_feature_names_out()
                    feature_importances = model_fitted.feature_importances_
                    fi_data = [(feature, float(importance), experiment_name, model_name, offset)
                               for feature, importance in zip(feature_names, feature_importances)]
                    con.executemany("""
                        INSERT INTO feature_importance_log (feature, importance, experiment_name, model_name, chunk_offset)
                        VALUES (?, ?, ?, ?, ?)
                    """, fi_data)
                    print(f"✅ Feature importances logged for {model_name} on chunk {offset}-{offset+limit}")
                except Exception as e:
                    print(f"❌ Feature importance logging failed: {e}")

            try:
                Xt_test = preprocessor.transform(X_test.compute())
                explainer = shap.TreeExplainer(model_fitted)
                shap_values = explainer.shap_values(Xt_test)

                preprocessed_feature_names = (
                    preprocessor.named_transformers_['num'].named_steps['scaler'].get_feature_names_out(numerical_cols).tolist() +
                    preprocessor.named_transformers_['cat'].named_steps['onehot'].get_feature_names_out(categorical_cols).tolist()
                )

                mean_shap = pd.DataFrame(shap_values, columns=preprocessed_feature_names).abs().mean().reset_index()
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
                print(f"❌ SHAP logging failed for chunk {offset}-{offset+limit}: {e}")

            logs.append((experiment_name, len(df_chunk), n_trials, model_name, json.dumps({}), auc, acc, f1, dpd, eod, 1))

        con.executemany("""
            INSERT INTO model_training_logs (experiment_name, sample_size, number_of_training_trials, model_name, best_params, auc, accuracy, f1, dpd, eod, used_strata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, logs)

        del df_chunk, ddf, X_train, X_test, y_train, y_test, pipeline, model_fitted
        gc.collect()

        return f"✅ Chunk {offset}-{offset+limit} processed successfully."

    except Exception as e:
        gc.collect()
        return f"❌ Chunk {offset}-{offset+limit} failed with error: {e}"

if __name__ == "__main__":
    cluster = LocalCluster(n_workers=2, threads_per_worker=2, memory_limit='6GB', dashboard_address=':8787')
    client = Client(cluster)
    client.run(gc.collect)
    print(client)

    token = os.getenv('MOTHERDUCK_TOKEN')
    experiment_name = f"experiment_{datetime.now().strftime('%Y%m%d_%H%M%s')}"

    total_rows = con.execute("SELECT count(*) FROM stg_hmda_training_sampled WHERE strata_bin IS NOT NULL").fetchone()[0]
    sample_rows = int(total_rows * sample_frac)
    print(f"Total rows: {total_rows}, Sampling {sample_frac*100:.1f}% => {sample_rows}")

    offsets = list(range(0, sample_rows, chunk_size))
    futures = []

    for offset in offsets:
        future = client.submit(process_chunk, offset, min(chunk_size, sample_rows - offset), token, experiment_name)
        futures.append(future)
        if len(futures) >= 3:
            for f in futures:
                print(f.result())
            futures = []

    for f in futures:
        print(f.result())
