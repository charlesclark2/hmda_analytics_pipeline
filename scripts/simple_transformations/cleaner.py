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

def clean_dataframe(df):
    df.loc[df['state_code'] == 'nan', 'state_code'] = np.nan

    df = to_int64(df, 'county_code')
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
    df['loan_purpose_category'] = df['loan_purpose'].map(loan_purpose_map)
    df['lien_status_category'] = df['lien_status'].map(lien_status_map)

    df['reverse_mortgage_category'] = df['reverse_mortgage'].map({1: 'Reverse Mortgage', 2: 'Not Reverse Mortgage', 1111: 'Exempt'})
    df = df.replace({'reverse_mortgage': {1: 1, 2: 0, 1111: np.nan}})

    df = replace_and_map(df, 'open_end_line_of_credit', {
        1: ('Open Line of Credit', 1),
        2: ('Not Open Line of Credit', 0),
        1111: ('Exempt', np.nan)
    })

    df = replace_and_map(df, 'business_or_commercial_purpose', {
        1: ('Business Purpose', 1),
        2: ('Not Business Purpose', 0),
        1111: ('Exempt', np.nan)
    })

    # Handle loan-to-value logic
    if 'loan_to_value_ratio' in df.columns.tolist() and 'combined_loan_to_value_ratio' in df.columns.tolist():
        df['combined_loan_to_value_ratio'] = pd.to_numeric(df['combined_loan_to_value_ratio'], errors='coerce')
        df['effective_ltv'] = df['loan_to_value_ratio']
        df.loc[df['effective_ltv'].isna(), 'effective_ltv'] = df['combined_loan_to_value_ratio']
    elif 'loan_to_value_ratio' in df.columns.tolist() and 'combined_loan_to_value_ratio' not in df.columns.tolist():
        df['combined_loan_to_value_ratio'] = np.nan
        df['effective_ltv'] = df['loan_to_value_ratio']
        df.loc[df['effective_ltv'].isna(), 'effective_ltv'] = df['combined_loan_to_value_ratio']
    elif 'combined_loan_to_value_ratio' in df.columns.tolist() and 'loan_to_value_ratio' not in df.columns.tolist():
        df['combined_loan_to_value_ratio'] = pd.to_numeric(df['combined_loan_to_value_ratio'], errors='coerce')
        df['effective_ltv'] = df['combined_loan_to_value_ratio']
    else:
        df['effective_ltv'] = np.nan

    for col in ['interest_rate', 'rate_spread']:
        df = clean_column(df, col)

    df['hopea_status_category'] = df['hoepa_status'].map(hoepa_status_map)
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

    return df
