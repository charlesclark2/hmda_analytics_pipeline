# data_cleaning.py

import pandas as pd
import numpy as np
import warnings

def clean_column(df, col, allow_negative=True, verbose=False):
    """
    Cleans a numeric column by:
    - Replacing common non-numeric strings with NaN
    - Stripping whitespace
    - Converting to float
    - Optionally dropping negatives (e.g., for income, LTV, etc.)
    """
    if col not in df.columns:
        return df

    df[col] = df[col].where(df[col].isna(), df[col].astype(str).str.strip())
    original_nulls = df[col].isna().sum()
    df[col] = pd.to_numeric(df[col], errors='coerce')
    if verbose:
        new_nulls = df[col].isna().sum()
        print(f"[clean_column] Column '{col}': coerced {new_nulls - original_nulls} values to NaN")
    if not allow_negative:
        df.loc[df[col] < 0, col] = np.nan

    return df

def clean_categorical_column(df, col, standardize_case=True, replacements=None, drop_null_like=True, title_case=False):
    """
    Cleans up categorical (string) columns by:
    - Stripping whitespace
    - Lowercasing or title-casing (optional)
    - Replacing known "null-like" values with NaN
    - Applying optional replacement mappings (e.g., {"Y": "Yes", "N": "No"})
    """
    if col not in df.columns:
        return df

    df[col] = df[col].astype(str).str.strip()

    if drop_null_like:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', FutureWarning)
            df[col] = df[col].replace(
                ["", "nan", "null", "none", "na", "n/a", "exempt", ".", "-", "unknown"],
                np.nan,
                regex=True
            ).infer_objects(copy=False)


    if title_case:
        df[col] = df[col].where(df[col].isna(), df[col].astype(str).str.strip().str.title())
    elif standardize_case:
        df[col] = df[col].where(df[col].isna(), df[col].astype(str).str.strip().str.lower())

    if replacements:
        df[col] = df[col].replace(replacements)

    return df

def clean_categorical_series(series, standardize_case=True, replacements=None, drop_null_like=True, title_case=False):
    """
    Cleans a pandas Series of categorical values with similar logic to clean_categorical_column.
    """
    s = series.where(series.isna(), series.astype(str).str.strip())
    

    if drop_null_like:
        s = s.replace(
            ["", "nan", "null", "none", "na", "n/a", "exempt", ".", "-", "unknown"],
            np.nan,
            regex=True
        )

    if title_case:
        s = s.where(s.isna(), s.astype(str).str.strip().str.title())
    elif standardize_case:
        s = s.where(s.isna(), s.astype(str).str.strip().str.lower())

    if replacements:
        s = s.replace(replacements)

    return s