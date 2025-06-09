# Dask imports 
import dask.dataframe as dd 
from dask.distributed import Client, LocalCluster 

from sqlalchemy import create_engine, Table, Column, Integer, String, Float, JSON, ARRAY, MetaData, TIMESTAMP
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
# standard imports
import time 
import os 
import logging

import warnings
from pandas.errors import PerformanceWarning
import duckdb 
from dotenv import load_dotenv
import json 
from datetime import datetime 

# Setup environment
load_dotenv()
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
warnings.filterwarnings("ignore")

# Connect to MotherDuck
print("Connecting to MotherDuck...")
con = duckdb.connect(f"motherduck:my_db?motherduck_token={motherduck_token}")

sample_frac = .001
target = 'loan_approved'
chunk_size = 100_00
parquet_partitions = 4
n_trials = 10

experiment_name = f"experiment_{datetime.now().strftime('%Y%m%d_%H%M%s')}"

if __name__ == "__main__":
    cluster = LocalCluster(
        n_workers=3, 
        threads_per_worker=2, 
        memory_limit='4GB', 
        dashboard_address=':8787'
    )
    client = Client(cluster)
    print(client)

    print("Preparing stratified sampling query in SQL")

    # sql_base = f"""
    # with stratified_sample as (
    #     select *
    #     from (
    #         select
    #             *, 
    #             row_number() over (partition by {target} order by random()) as rn, 
    #             count(*) over (partition by {target}) as total_rows
    #         from mart_hmda_preprocessed
    #         where {target} is not null 
    #         limit 1000000
    #     )
    #     where rn <= total_rows * {sample_frac}
    # )
    # select *
    # from stratified_sample 
    # """
    sql_base = f"""
    with sampled as (
        select
            *
        from stg_hmda_training_with_strata
        where strata_bin is not null 
    )
    select *
    from sampled
    using sample reservoir(2000000)
    """

    total_sample_rows = con.execute(f"select count(*) from ({sql_base})").fetchone()[0]
    print(f"Total sampled rows: {total_sample_rows}")

    dfs = []
    offset = 0
    rows_loaded = 0

    # start_time = time.time()
    # while offset < total_sample_rows: 
    #     # sql_chunk = f"{sql_base} limit {chunk_size} offset {offset}"
    #     df_chunk = con.execute(sql_chunk).fetchdf()
    #     chunk_rows = len(df_chunk)

    #     if chunk_rows == 0: 
    #         print(f"Warning: no rows feteched at {offset}")
    #         break 

    #     dfs.append(df_chunk)
    #     rows_loaded += chunk_rows 
    #     percent_complete = (rows_loaded / total_sample_rows) * 100 

    #     elapsed_time = time.time() - start_time
    #     print(f"Chunk loaded: {chunk_rows} rows | Cumulative: {rows_loaded}/{total_sample_rows} ({percent_complete:.2f}%) | Elapsed Time: {elapsed_time:.2f}s")

    #     offset += chunk_size

    # if not dfs: 
    #     raise ValueError("No data loaded from DuckDB stratified sample")

    # full_df = pd.concat(dfs, ignore_index=True)
    # ddf = dd.from_pandas(full_df, npartitions=parquet_partitions)

    pandas_df = con.execute(sql_base).fetchdf()
    ddf = dd.from_pandas(pandas_df, npartitions=parquet_partitions)
    print(f"Total rows loaded into Dask: {len(pandas_df)}")

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
        print(f"AUC-ROC for {name}: {auc:.4f}")

    top_models = sorted(initial_results, key=initial_results.get, reverse=True)[:2]
    print(f"\n🔝 Top 2 models: {top_models}")
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
                params = {
                    'C': trial.suggest_float('C', 1e-3, 10.0, log=True),
                    'penalty': trial.suggest_categorical('penalty', ['l2', 'elasticnet']),
                    'solver': trial.suggest_categorical('solver', ['saga', 'lbfgs']),
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
        
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials)
        best_params = study.best_params
        print(f"✅ Best hyperparameters for {model_name}: {best_params}")

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
        metric_frame = MetricFrame(metrics={'accuracy': accuracy_score, 'selection_rate': selection_rate},
                                   y_true=y_test.compute(), y_pred=y_pred,
                                   sensitive_features=sensitive_feature)
        dpd = demographic_parity_difference(y_test.compute(), y_pred, sensitive_features=sensitive_feature)
        eod = equalized_odds_difference(y_test.compute(), y_pred, sensitive_features=sensitive_feature)

        print(f"📊 Fairness Metrics for {model_name}:")
        print(metric_frame.by_group)
        print(f"Demographic Parity Difference: {dpd:.4f}")
        print(f"Equalized Odds Difference: {eod:.4f}")

        # Insert logging into DuckDB
        con.execute("""
            INSERT INTO model_training_logs (experiment_name, sample_size, number_of_training_trials, model_name, best_params, auc, accuracy, f1, dpd, eod, used_strata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            experiment_name,
            len(pandas_df),
            n_trials,
            model_name,
            json.dumps(best_params),
            auc,
            acc,
            f1,
            dpd,
            eod, 
            1
        ))
        print(f"✅ Logged metrics for {model_name} under experiment {experiment_name}.")

