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
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Suppress PerformanceWarning
warnings.simplefilter(action="ignore", category=PerformanceWarning)

os.environ["DASK_DISTRIBUTED__LOGGING__DISTRIBUTED_UTILS_PERF"] = "error"



parquet_path = "/Volumes/T9/hmda_project/full_dataset/"
sample_path = "/Volumes/T9/hmda_project/sample_data"
sample_frac = .0001
target = 'loan_approved'

USE_SAMPLE = False 
RESAMPLE = False 
base_path = "/Volumes/T9/hmda_project/full_dataset"
# Example: load a specific partition


if __name__ == '__main__':
    # Suppress "distributed.utils_perf" warnings
    cluster = LocalCluster(
        n_workers=3, 
        threads_per_worker=2, 
        memory_limit='4GB', 
        dashboard_address=':8787'
    )
    client = Client(cluster)
    print(client)

    start_time = time.time()

    # if USE_SAMPLE and os.path.exists(sample_path) and not RESAMPLE: 
    #     print(f"Loading sampled data from: {sample_path }")
    #     ddf = dd.read_parquet(sample_path, engine='pyarrow')
    #     ddf = ddf.sample(frac=0.01, random_state=42)
    # else: 
    #     print("Loading full dataset from: ", parquet_path)
    #     ddf = dd.read_parquet(parquet_path, engine='pyarrow')
    #     print(f"Total rows: ~{ddf.shape[0].compute()}")
    #     print(f"Columns: {ddf.columns.tolist()}")
    
    #     if sample_frac < 1.0: 
    #         ddf = ddf.sample(frac=sample_frac, random_state=42)
    #         print(f"Sampled {sample_frac*100}% of the data (~{ddf.shape[0].compute()} rows)")

    #         if not os.path.exists(sample_path): 
    #             os.makedirs(sample_path)
    #         ddf.to_parquet(sample_path, write_index=False)
    #         print(f"Sampled data")

    load_time = time.time() - start_time
    print(f"Data loaded in {load_time:.2f} seconds")
    print(f"Total rows: ~{ddf.shape[0].compute()}")

    # precheck the data to see where the missing counts are located
    missing_counts = ddf.isnull().sum().compute()
    print("Missing values per column: ")
    print(missing_counts[missing_counts > 0])

    print(f"Columns in set: {ddf.columns}")

    # prepare the data
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
        
    # Select top 2 models
    top_models = sorted(initial_results, key=initial_results.get, reverse=True)[:2]
    print(f"\n🔝 Top 2 models: {top_models}")

    tuning_logs = []
    # Optuna tuning and logging
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
        study.optimize(objective, n_trials=10)
        best_params = study.best_params
        print(f"✅ Best hyperparameters for {model_name}: {best_params}")

        if model_name == "RandomForestClassifier":
            model = RandomForestClassifier(**best_params)
        elif model_name == "XGBoostClassifier":
            model = xgb.XGBClassifier(**best_params)
        else:
            continue

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

        selected_features = numerical_cols + categorical_cols
        print(f"Selected features: {selected_features}")
        data = {
            "name": model_name, 
            "best_params": best_params, 
            "auc": auc, 
            "acc": acc, 
            "f1": f1, 
            "dpd": dpd, 
            "eod": eod, 
            "selected_features": selected_features 
        }
        tuning_logs.append(data)
        print("BEST PARAMETERS")
        print(data)
    print(tuning_logs)

