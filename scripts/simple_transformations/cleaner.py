# cleaner.py
import pandas as pd
import numpy as np
from mappings import (
    purchaser_map, loan_type_map, loan_purpose_map,
    lien_status_map, hoepa_status_map
)
from transforms import (
    replace_and_map, clean_column, to_int64, multiply_column,
    standardize_column_values, bucket_total_units, transform_dti
)
from data_cleaning import (
    clean_column, clean_categorical_column
)

# Define reusable canonical schema for downstream validation
canonical_schema = {
    'state_code': 'string',
    'county_code': 'string',
    'conforming_loan_limit': 'Int64',
    'loan_to_value_ratio': 'float64',
    'combined_loan_to_value_ratio': 'float64',
    'effective_ltv': 'float64',
    'interest_rate': 'float64',
    'rate_spread': 'float64',
    'income': 'float64',
    'loan_term': 'Int64',
    'prepayment_penalty_term': 'Int64',
    'intro_rate_period': 'Int64',
    'total_loan_costs': 'float64',
    'total_points_and_fees': 'float64',
    'origination_charges': 'float64',
    'discount_points': 'float64',
    'lender_credits': 'float64'
}

def enforce_column_types(df):
    for col, dtype in canonical_schema.items():
        if col not in df.columns:
            # Add missing column with NaN values of appropriate dtype
            if dtype == "Int64":
                df[col] = pd.Series([pd.NA] * len(df), dtype="Int64")
            elif dtype == "float64":
                df[col] = pd.Series([np.nan] * len(df), dtype="float64")
            else:
                df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)
        else:
            try:
                df[col] = df[col].astype(dtype)
            except Exception:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)

    # Convert all *_category and *_bucketed columns to string
    for col in df.columns:
        if col.endswith("_category") or col.endswith("_bucketed"):
            df[col] = df[col].astype(str)

    return df

def validate_schema(df):
    """
    Raises an error if df does not conform to canonical schema.
    """
    for col, expected_dtype in canonical_schema.items():
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
        actual_dtype = str(df[col].dtype)
        if actual_dtype != expected_dtype:
            raise TypeError(f"Column {col} has dtype {actual_dtype}, expected {expected_dtype}")

def clean_dataframe(df):
    df.loc[df['state_code'] == 'nan', 'state_code'] = np.nan
    df['state_code'] = df['state_code'].astype(str)
    df['county_code'] = df['county_code'].astype(str)

    df = standardize_column_values(df, 'conforming_loan_limit', {'C': 1, 'NC': 0})
    df = to_int64(df, 'conforming_loan_limit')

    df['action_taken_category'] = df['action_taken'].map({
        1: 'Origination',
        2: 'Fallout',
        3: 'Denied',
        4: 'Fallout',
        5: 'Fallout',
        6: 'Purchased - Secondary Market',
        7: 'Preapproval - Denial',
        8: 'Preapproval - Approved not accepted'
    })

    df['purchaser_category'] = df['purchaser_type'].map(purchaser_map)
    df.loc[df['preapproval'] == 2, 'preapproval'] = 0
    df['loan_type_category'] = df['loan_type'].map(loan_type_map)
    df = clean_categorical_column(df, 'loan_type_category', title_case=True)
    df['loan_purpose_category'] = df['loan_purpose'].map(loan_purpose_map)
    df = clean_categorical_column(df, 'loan_purpose_category', title_case=True)
    df['lien_status_category'] = df['lien_status'].map(lien_status_map)
    df = clean_categorical_column(df, 'lien_status_category', title_case=True)

    df['reverse_mortgage_category'] = df['reverse_mortgage'].map({1: 'Reverse Mortgage', 2: 'Not Reverse Mortgage', 1111: 'Exempt'})
    df = clean_categorical_column(df, 'reverse_mortgage_category', title_case=True)
    df = df.replace({'reverse_mortgage': {1: 1, 2: 0, 1111: np.nan}})

    df = replace_and_map(df, 'open_end_line_of_credit', {
        1: ('Open Line Of Credit', 1),
        2: ('Not Open Line Of Credit', 0),
        1111: ('Exempt', np.nan)
    })
    df = clean_categorical_column(df, 'open_end_line_of_credit_category', title_case=True)

    df = replace_and_map(df, 'business_or_commercial_purpose', {
        1: ('Business Purpose', 1),
        2: ('Not Business Purpose', 0),
        1111: ('Exempt', np.nan)
    })
    df = clean_categorical_column(df, 'business_or_commercial_purpose_category', title_case=True)

    if 'loan_to_value_ratio' in df.columns:
        df['loan_to_value_ratio'] = pd.to_numeric(df['loan_to_value_ratio'], errors='coerce')
    else: 
        df['loan_to_value_ratio'] = np.nan
    if 'combined_loan_to_value_ratio' in df.columns:
        df['combined_loan_to_value_ratio'] = pd.to_numeric(df['combined_loan_to_value_ratio'], errors='coerce')
    else: 
        df['combined_laon_to_value_ratio'] = np.nan

    df['effective_ltv'] = df['loan_to_value_ratio']
    if 'combined_loan_to_value_ratio' in df.columns:
        df.loc[df['effective_ltv'].isna(), 'effective_ltv'] = df['combined_loan_to_value_ratio']

    for col in ['interest_rate', 'rate_spread']:
        df = clean_column(df, col)

    df['hopea_status_category'] = df['hoepa_status'].map(hoepa_status_map)
    df = clean_categorical_column(df, 'hopea_status_category', title_case=True)
    df = df.replace({'hoepa_status': {1: 1, 2: 0, 3: np.nan}})

    for col in [
        'total_loan_costs', 'total_points_and_fees', 'origination_charges',
        'discount_points', 'lender_credits'
    ]:
        df = clean_column(df, col)

    for col in ['loan_term', 'prepayment_penalty_term', 'intro_rate_period']:
        df = to_int64(df, col, replace_exempt=True)

    df['total_units_bucketed'] = df['total_units'].apply(bucket_total_units)
    df = clean_column(df, 'income')
    df = multiply_column(df, 'income', 1000)
    df['debt_to_income_ratio'] = df['debt_to_income_ratio'].apply(transform_dti)

    df = enforce_column_types(df)
    validate_schema(df)

    return df
