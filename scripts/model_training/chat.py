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
sql_chunk = f"""
        SELECT *
        FROM stg_training_sample_stratified
        limit 100000
        """
df_chunk = con.execute(sql_chunk).fetchdf()
for col in df_chunk.columns: 
    str_type = str(df_chunk[col].dtype)
    if col == 'lender_entity_name': 
        continue 
    if str_type in ['category', 'object']:
        print(f"Column: {col}, Data Type: {df_chunk[col].dtype}")
        print(df_chunk[col].unique().tolist())