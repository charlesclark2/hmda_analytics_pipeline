# transforms.py
import pandas as pd
import numpy as np

def replace_and_map(df, col, mapping):
    cat_col = f"{col}_category"
    df[cat_col] = df[col].map({k: v[0] for k, v in mapping.items()})
    df[col] = df[col].map({k: v[1] for k, v in mapping.items()})
    return df

def clean_column(df, col, allow_negative=True):
    """
    Cleans a numeric column by:
    - Replacing common non-numeric strings with NaN
    - Stripping whitespace
    - Converting to float
    - Optionally dropping negatives (e.g., for income, LTV, etc.)
    """
    if col not in df.columns:
        return df

    # Convert all values to string to normalize text-based nulls
    df[col] = df[col].astype(str).str.strip()

    # Replace known non-numeric flags
    df[col] = df[col].replace(
        to_replace=["nan", "NaN", "NAN", "NA", "Exempt", "None", "null", ""],
        value=np.nan
    )

    # Now convert to float
    df[col] = pd.to_numeric(df[col], errors='coerce')

    # Optionally drop invalid negative values
    if not allow_negative:
        df.loc[df[col] < 0, col] = np.nan

    return df


def to_int64(df, col, replace_exempt=False):
    if replace_exempt:
        df.loc[df[col].isin(['nan', 'Exempt']), col] = np.nan
    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    return df

def multiply_column(df, col, factor):
    df[col] = df[col] * factor
    return df

def standardize_column_values(df, col, mapping):
    df[col] = df[col].replace(mapping)
    return df

def bucket_total_units(code):
    if pd.isna(code):
        return "Unknown"
    try:
        val = int(code)
        return str(val) if val <= 4 else "5+"
    except:
        return "5+"

def transform_dti(x):
    x = str(x)
    if x in ['nan', 'Exempt']:
        return np.nan
    try:
        val = int(x)
        if val < 20:
            return '<20%'
        elif val < 30:
            return '20%-<30%'
        elif val < 36:
            return '30%-<36%'
        elif val < 40:
            return '36%-<40%'
        elif val < 46:
            return '40%-<46%'
        elif val < 50:
            return '46%-<50%'
        elif val <= 60:
            return '50%-60%'
        else:
            return '>60%'
    except:
        return x
