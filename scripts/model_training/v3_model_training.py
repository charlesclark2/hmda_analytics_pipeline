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
from dask_ml.model_selection import train_test_split
from dotenv import load_dotenv
import os, json, time, duckdb
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

# Setup environment
load_dotenv()
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
warnings.filterwarnings("ignore")
print("Connecting to MotherDuck...")
con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")

sample_frac = 0.05  # 10%
target = 'loan_approved'
chunk_size = 100_000  # Adjustable
parquet_partitions = 4
n_trials = 10
experiment_name = f"experiment_{datetime.now().strftime('%Y%m%d_%H%M%s')}"

# Connect to Dask cluster


def process_chunk(offset, limit, motherduck_token, experiment_name): 
    try:
        import duckdb
        import os
        import dask.dataframe as dd
        import json
        from sklearn.pipeline import Pipeline
        from sklearn.linear_model import LogisticRegression
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.tree import DecisionTreeClassifier
        from catboost import CatBoostClassifier
        import xgboost as xgb
        from sklearn.metrics import roc_auc_score, accuracy_score, f1_score
        from sklearn.impute import SimpleImputer
        from sklearn.preprocessing import StandardScaler, OneHotEncoder
        from sklearn.compose import ColumnTransformer
        from sklearn.model_selection import cross_val_score
        from fairlearn.metrics import (
            demographic_parity_difference,
            equalized_odds_difference,
        )
        import duckdb 
        from dotenv import load_dotenv
        load_dotenv()
        motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
        warnings.filterwarnings("ignore")
        print("Connecting to MotherDuck...")
        con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")
        sql_chunk = f"""
        SELECT *
            FROM stg_hmda_training_with_strata
            WHERE strata_bin IS NOT NULL
            LIMIT {limit} OFFSET {offset}
        """
        df_chunk = con.execute(sql_chunk).fetchdf()
        if df_chunk.empty: 
            return "No rows in this chunk"
        
        ddf = dd.from_pandas(df_chunk, npartitions=parquet_partitions)
        print(f"Total rows loaded into Dask: {len(df_chunk)}")

        # Prepare data
        exclude_cols = ['loan_application_id', 'census_tract', 'zip_code', 'county_code', 'state_code', target]
        X = ddf.drop(columns=exclude_cols)
        y = ddf[target]
        categorical_cols = X.select_dtypes(include=['object', 'category']).columns.tolist()
        numerical_cols = X.select_dtypes(include=['number']).columns.tolist()

        preprocessor = ColumnTransformer(transformers=[
            ('num', Pipeline([
                ('imputer', SimpleImputer(strategy='median')),  # Impute missing numeric values
                ('scaler', StandardScaler())
            ]), numerical_cols),
            ('cat', Pipeline([
                ('imputer', SimpleImputer(strategy='most_frequent')),  # Impute missing categorical values
                ('onehot', OneHotEncoder(handle_unknown='ignore'))
            ]), categorical_cols)
        ])

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=True)

        if len(y_train.compute().unique()) < 2:
            return f"⚠️ Skipping chunk {offset}: only one class present."
        
        # initial models
        print("Starting the model training process")
        models = {
            "LogisticRegression": LogisticRegression(max_iter=1000), 
            "RandomForestClassifier": RandomForestClassifier(n_estimators=100), 
            "DecisionTreeClassifier": DecisionTreeClassifier(),
            "CatBoostClassifier": CatBoostClassifier(verbose=100),
            "XGBoostClassifier": xgb.XGBClassifier(use_label_encoder=False, eval_metric='logloss')
        }

        initial_results = {}

        for name, model in models.items(): 
            print(f"Training: {name}")
            pipeline = Pipeline([('preprocessor', preprocessor), ('model', model)])
            pipeline.fit(X_train.compute(), y_train.compute())
            y_pred = pipeline.predict(X_test.compute())
            auc = roc_auc_score(y_test.compute(), y_pred)
            initial_results[name] = auc
            print(f"📈 AUC-ROC for {name} on chunk {offset}-{offset+limit}: {auc:.4f}")

        top_models = [k for k, v in sorted(initial_results.items(), key=lambda item: item[1], reverse=True)[:2]]
        print(f"\n🔝 Top 2 models: {top_models}")
        logs = []
        for model_name in top_models:
            print(f"\n🔍 Tuning hyperparameters for {model_name} using Optuna...")
            def objective(trial):
                if model_name == "RandomForestClassifier":
                    params = {
                        'n_estimators': trial.suggest_int('n_estimators', 50, 300),
                        'max_depth': trial.suggest_int('max_depth', 3, 20),
                        'min_samples_split': trial.suggest_int('min_samples_split', 2, 10),
                        'random_state': 42
                    }
                    model = RandomForestClassifier(**params)
                
                elif model_name == "XGBoostClassifier":
                    params = {
                        'n_estimators': trial.suggest_int('n_estimators', 50, 300),
                        'max_depth': trial.suggest_int('max_depth', 3, 20),
                        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                        'subsample': trial.suggest_float('subsample', 0.5, 1.0),
                        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
                        'use_label_encoder': False,
                        'eval_metric': 'logloss'
                    }
                    model = xgb.XGBClassifier(**params)

                elif model_name == "LogisticRegression":
                    solver = trial.suggest_categorical('solver', ['saga', 'lbfgs'])
                    if solver == 'lbfgs':
                        penalty = 'l2'
                    else:
                        penalty = trial.suggest_categorical('penalty', ['l2', 'elasticnet'])

                    params = {
                        'C': trial.suggest_float('C', 1e-3, 10.0, log=True),
                        'penalty': penalty,
                        'solver': solver,
                        'max_iter': 1000
                    }
                    model = LogisticRegression(**params)

                elif model_name == "DecisionTreeClassifier":
                    params = {
                        'max_depth': trial.suggest_int('max_depth', 3, 20),
                        'min_samples_split': trial.suggest_int('min_samples_split', 2, 10),
                        'criterion': trial.suggest_categorical('criterion', ['gini', 'entropy'])
                    }
                    model = DecisionTreeClassifier(**params)

                elif model_name == "CatBoostClassifier":
                    params = {
                        'iterations': trial.suggest_int('iterations', 100, 1000),
                        'depth': trial.suggest_int('depth', 3, 10),
                        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                        'verbose': 0
                    }
                    model = CatBoostClassifier(**params)
                
                else:
                    return 0  # Default for unsupported models

                # Pipeline with preprocessing
                pipeline = Pipeline([('preprocessor', preprocessor), ('model', model)])
                
                auc = cross_val_score(
                    pipeline,
                    X_train.compute(),
                    y_train.compute(),
                    cv=3,
                    scoring='roc_auc'
                ).mean()

                return auc
            
            import optuna 
            study = optuna.create_study(direction='maximize')
            study.optimize(objective, n_trials=n_trials)
            best_params = study.best_params
            print(f"✅ Best hyperparameters for {model_name}: {best_params}")
            study = optuna.create_study(direction='maximize')
            study.optimize(objective, n_trials=n_trials)
            best_params = study.best_params

            if model_name == "RandomForestClassifier":
                model = RandomForestClassifier(**best_params)
            elif model_name == "XGBoostClassifier":
                model = xgb.XGBClassifier(**best_params)
            elif model_name == "CatBoostClassifier":
                model = CatBoostClassifier(**best_params)
            elif model_name == "LogisticRegression":
                model = LogisticRegression(**best_params)
            elif model_name == "DecisionTreeClassifier":
                model  = DecisionTreeClassifier(**best_params)
            else:
                raise ValueError(f"Unsupported model: {model_name}")

            pipeline = Pipeline([('preprocessor', preprocessor), ('model', model)])
            pipeline.fit(X_train.compute(), y_train.compute())
            y_pred = pipeline.predict(X_test.compute())
            auc = roc_auc_score(y_test.compute(), y_pred)
            acc = accuracy_score(y_test.compute(), y_pred)
            f1 = f1_score(y_test.compute(), y_pred)
            sensitive_feature = X_test.compute()['applicant_derived_racial_category']
            dpd = demographic_parity_difference(y_test.compute(), y_pred, sensitive_features=sensitive_feature)
            eod = equalized_odds_difference(y_test.compute(), y_pred, sensitive_features=sensitive_feature)
            logs.append((experiment_name, len(df_chunk), n_trials, model_name, json.dumps(best_params), auc, acc, f1, dpd, eod, 1))
        con.executemany("""
            INSERT INTO model_training_logs (
                experiment_name, sample_size, number_of_training_trials,
                model_name, best_params, auc, accuracy, f1, dpd, eod, used_strata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, logs)
        return f"✅ Chunk {offset}-{offset+limit} processed successfully."
    except Exception as e: 
        error_msg = f"Error processing chunk - {offset}-{offset+limit}: {str(e)}"
        print(error_msg)
        return error_msg 


if __name__ == "__main__":
    load_dotenv()
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")
    total_rows = con.execute("SELECT COUNT(*) FROM stg_hmda_training_with_strata WHERE strata_bin IS NOT NULL").fetchone()[0]
    sample_rows = int(total_rows * sample_frac)
    print(f"Total rows: {total_rows} | Sample: {sample_rows}")

    cluster = LocalCluster(n_workers=3, threads_per_worker=2, memory_limit='4GB', dashboard_address=':8787')
    client = Client(cluster)

    offsets = list(range(0, sample_rows, chunk_size))
    futures = [
        client.submit(process_chunk, offset, min(chunk_size, sample_rows - offset), motherduck_token, experiment_name)
        for offset in offsets
    ]
    for future in futures:
        print(future.result())
