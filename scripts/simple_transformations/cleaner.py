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
from hmda_schema import canonical_schema

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

    # Other categorical features 
    df.loc[df['negative_amortization'] == 1, 'negative_amortization_category'] = 'Negative Amortization Features'
    df.loc[df['negative_amortization'] == 2, 'negative_amortization_category'] = 'No Negative Amortization Features'
    df.loc[df['negative_amortization'] == 1111, 'negative_amortization_category'] = np.nan
    df['negative_amortization_category'] = df['negative_amortization_category'].astype(str)

    df.loc[df['negative_amortization'] == 1, 'negative_amortization'] = 1
    df.loc[df['negative_amortization'] == 2, 'negative_amortization'] = 0
    df.loc[df['negative_amortization'] == 1111, 'negative_amortization'] = np.nan

    df.loc[df['interest_only_payment'] == 1, 'interest_only_payment_category'] = 'Interest-Only Payments'
    df.loc[df['interest_only_payment'] == 2, 'interest_only_payment_category'] = 'No Interest-Only Payments'
    df.loc[df['interest_only_payment'] == 1111, 'interest_only_payment_category'] = np.nan
    df['interest_only_payment_category'] = df['interest_only_payment_category'].astype(str)

    df.loc[df['interest_only_payment'] == 1, 'interest_only_payment'] = 1
    df.loc[df['interest_only_payment'] == 2, 'interest_only_payment'] = 0
    df.loc[df['interest_only_payment'] == 1111, 'interest_only_payment'] = np.nan

    df.loc[df['balloon_payment'] == 1, 'balloon_payment_category'] = 'Balloon Payment'
    df.loc[df['balloon_payment'] == 2, 'balloon_payment_category'] = 'No Balloon Payment'
    df.loc[df['balloon_payment'] == 1111, 'balloon_payment_category'] = np.nan
    df['balloon_payment_category'] = df['balloon_payment_category'].astype(str)

    df.loc[df['balloon_payment'] == 1, 'balloon_payment'] = 1
    df.loc[df['balloon_payment'] == 2, 'balloon_payment'] = 0
    df.loc[df['balloon_payment'] == 1111, 'balloon_payment'] = np.nan

    df.loc[df['other_nonamortizing_features'] == 1, 'other_nonamortizing_features_category'] = 'Other non-fully amortizing features'
    df.loc[df['other_nonamortizing_features'] == 2, 'other_nonamortizing_features_category'] = 'No other non-fully amortizing features'
    df.loc[df['other_nonamortizing_features'] == 1111, 'other_nonamortizing_features_category'] = np.nan
    df['other_nonamortizing_features_category'] = df['other_nonamortizing_features_category'].astype(str)

    df.loc[df['other_nonamortizing_features'] == 1, 'other_nonamortizing_features'] = 1
    df.loc[df['other_nonamortizing_features'] == 2, 'other_nonamortizing_features'] = 0
    df.loc[df['other_nonamortizing_features'] == 1111, 'other_nonamortizing_features'] = np.nan

    df.loc[df['property_value'].isin(['nan', 'Exempt']), 'property_value'] = np.nan
    df['property_value'] = pd.to_numeric(df['property_value'], errors='coerce').astype('Int64')

    df.loc[df['construction_method'] == 1, 'construction_method_category'] = 'Site-built'
    df.loc[df['construction_method'] == 2, 'construction_method_category'] = 'Manufactured Home'
    df['construction_method_category'] = df['construction_method_category'].astype(str)

    df.loc[df['occupancy_type'] == 1, 'occupancy_type_category'] = 'Principal Residence'
    df.loc[df['occupancy_type'] == 2, 'occupancy_type_category'] = 'Second Residence'
    df.loc[df['occupancy_type'] == 3, 'occupancy_type_category'] = 'Investment Property'
    df['occupancy_type_category'] = df['occupancy_type_category'].astype(str)

    df.loc[df['manufactured_home_secured_property_type'] == 1, 'manufactured_home_secured_property_type_category'] = 'Manufactured home and land'
    df.loc[df['manufactured_home_secured_property_type'] == 2, 'manufactured_home_secured_property_type_category'] = 'Manufactured Home - Not Land'
    df.loc[df['manufactured_home_secured_property_type'] == 3, 'manufactured_home_secured_property_type_category'] = np.nan
    df.loc[df['manufactured_home_secured_property_type'] == 1111, 'manufactured_home_secured_property_type_category'] = np.nan
    df['manufactured_home_secured_property_type_category'] = df['manufactured_home_secured_property_type_category'].astype(str)

    df.loc[df['manufactured_home_secured_property_type'] == 3, 'manufactured_home_secured_property_type'] = np.nan
    df.loc[df['manufactured_home_secured_property_type'] == 1111, 'manufactured_home_secured_property_type'] = np.nan

    df.loc[df['manufactured_home_land_property_interest'] == 1, 'manufactured_home_land_property_interest_category'] = 'Direct Ownership'
    df.loc[df['manufactured_home_land_property_interest'] == 2, 'manufactured_home_land_property_interest_category'] = 'Indirect Ownership'
    df.loc[df['manufactured_home_land_property_interest'] == 3, 'manufactured_home_land_property_interest_category'] = 'Paid Leasehold'
    df.loc[df['manufactured_home_land_property_interest'] == 4, 'manufactured_home_land_property_interest_category'] = 'Unpaid Leasehold'
    df.loc[df['manufactured_home_land_property_interest'] == 5, 'manufactured_home_land_property_interest_category'] = np.nan
    df.loc[df['manufactured_home_land_property_interest'] == 1111, 'manufactured_home_land_property_interest_category'] = np.nan
    df['manufactured_home_land_property_interest_category'] = df['manufactured_home_land_property_interest_category'].astype(str)

    df.loc[df['manufactured_home_land_property_interest'] == 5, 'manufactured_home_land_property_interest'] = np.nan
    df.loc[df['manufactured_home_land_property_interest'] == 1111, 'manufactured_home_land_property_interest'] = np.nan

    df["multifamily_affordable_units"] = pd.to_numeric(df["multifamily_affordable_units"], errors="coerce").astype('Int64')
    df["multifamily_affordable_units"] = df["multifamily_affordable_units"].fillna(0)

    df.loc[df['applicant_credit_score_type'] == 1, 'applicant_credit_score_type_category'] = 'Equifax Beacon 5.0'
    df.loc[df['applicant_credit_score_type'] == 2, 'applicant_credit_score_type_category'] = 'Experian Fair Isaac'
    df.loc[df['applicant_credit_score_type'] == 3, 'applicant_credit_score_type_category'] = 'FICO Risk Score Classic 04'
    df.loc[df['applicant_credit_score_type'] == 4, 'applicant_credit_score_type_category'] = 'FICO Risk Score Classic 98'
    df.loc[df['applicant_credit_score_type'] == 5, 'applicant_credit_score_type_category'] = 'VantageScore 2.0'
    df.loc[df['applicant_credit_score_type'] == 6, 'applicant_credit_score_type_category'] = 'VantageScore 3.0'
    df.loc[df['applicant_credit_score_type'] == 7, 'applicant_credit_score_type_category'] = 'More than one credit score model'
    df.loc[df['applicant_credit_score_type'] == 8, 'applicant_credit_score_type_category'] = 'Other credit scoring model'
    df.loc[df['applicant_credit_score_type'] == 9, 'applicant_credit_score_type_category'] = np.nan
    df.loc[df['applicant_credit_score_type'] == 1111, 'applicant_credit_score_type_category'] = np.nan
    df['applicant_credit_score_type_category'] = df['applicant_credit_score_type_category'].astype(str)

    df.loc[df['applicant_credit_score_type'] == 9, 'applicant_credit_score_type'] = np.nan
    df.loc[df['applicant_credit_score_type'] == 1111, 'applicant_credit_score_type'] = np.nan

    df.loc[df['co_applicant_credit_score_type'] == 1, 'co_applicant_credit_score_type_category'] = 'Equifax Beacon 5.0'
    df.loc[df['co_applicant_credit_score_type'] == 2, 'co_applicant_credit_score_type_category'] = 'Experian Fair Isaac'
    df.loc[df['co_applicant_credit_score_type'] == 3, 'co_applicant_credit_score_type_category'] = 'FICO Risk Score Classic 04'
    df.loc[df['co_applicant_credit_score_type'] == 4, 'co_applicant_credit_score_type_category'] = 'FICO Risk Score Classic 98'
    df.loc[df['co_applicant_credit_score_type'] == 5, 'co_applicant_credit_score_type_category'] = 'VantageScore 2.0'
    df.loc[df['co_applicant_credit_score_type'] == 6, 'co_applicant_credit_score_type_category'] = 'VantageScore 3.0'
    df.loc[df['co_applicant_credit_score_type'] == 7, 'co_applicant_credit_score_type_category'] = 'More than one credit score model'
    df.loc[df['co_applicant_credit_score_type'] == 8, 'co_applicant_credit_score_type_category'] = 'Other credit scoring model'
    df.loc[df['co_applicant_credit_score_type'] == 9, 'co_applicant_credit_score_type_category'] = 'No Co-Applicant'
    df.loc[df['co_applicant_credit_score_type'] == 10, 'co_applicant_credit_score_type_category'] = np.nan
    df.loc[df['co_applicant_credit_score_type'] == 1111, 'co_applicant_credit_score_type_category'] = np.nan
    df['co_applicant_credit_score_type_category'] = df['co_applicant_credit_score_type_category'].astype(str)

    df.loc[df['co_applicant_credit_score_type'] == 9, 'co_applicant_credit_score_type'] = np.nan
    df.loc[df['co_applicant_credit_score_type'] == 1111, 'co_applicant_credit_score_type'] = np.nan

    # Defragmenting
    df = df.copy()

    for x in range(1, 6): 
        df.loc[df[f"applicant_ethnicity_{x}"] == 1, f"applicant_ethnicity_{x}_category"] = 'Hispanic or Latino'
        df.loc[df[f"applicant_ethnicity_{x}"] == 2, f"applicant_ethnicity_{x}_category"] = 'Not Hispanic or Latino'
        df.loc[df[f"applicant_ethnicity_{x}"] == 3, f"applicant_ethnicity_{x}_category"] = np.nan
        df.loc[df[f"applicant_ethnicity_{x}"] == 4, f"applicant_ethnicity_{x}_category"] = np.nan
        df.loc[df[f"applicant_ethnicity_{x}"] == 11, f"applicant_ethnicity_{x}_category"] = 'Mexican'
        df.loc[df[f"applicant_ethnicity_{x}"] == 12, f"applicant_ethnicity_{x}_category"] = 'Puerto Rican'
        df.loc[df[f"applicant_ethnicity_{x}"] == 13, f"applicant_ethnicity_{x}_category"] = 'Cuban'
        df.loc[df[f"applicant_ethnicity_{x}"] == 14, f"applicant_ethnicity_{x}_category"] = 'Other Hispanic or Latino'
        df[f'applicant_ethnicity_{x}_category'] = df[f"applicant_ethnicity_{x}_category"].astype(str)
        
        df.loc[df[f"applicant_ethnicity_{x}"] == 3, f"applicant_ethnicity_{x}"] = np.nan
        df.loc[df[f"applicant_ethnicity_{x}"] == 4, f"applicant_ethnicity_{x}"] = np.nan
        df[f"applicant_ethnicity_{x}"] = pd.to_numeric(df[f"applicant_ethnicity_{x}"], errors="coerce").astype('Int64')
        
        df.loc[df[f"co_applicant_ethnicity_{x}"] == 1, f"co_applicant_ethnicity_{x}_category"] = 'Hispanic or Latino'
        df.loc[df[f"co_applicant_ethnicity_{x}"] == 2, f"co_applicant_ethnicity_{x}_category"] = 'Not Hispanic or Latino'
        df.loc[df[f"co_applicant_ethnicity_{x}"] == 3, f"co_applicant_ethnicity_{x}_category"] = np.nan
        df.loc[df[f"co_applicant_ethnicity_{x}"] == 4, f"co_applicant_ethnicity_{x}_category"] = np.nan
        df.loc[df[f"co_applicant_ethnicity_{x}"] == 5, f"co_applicant_ethnicity_{x}_category"] = np.nan
        df.loc[df[f"co_applicant_ethnicity_{x}"] == 11, f"co_applicant_ethnicity_{x}_category"] = 'Mexican'
        df.loc[df[f"co_applicant_ethnicity_{x}"] == 12, f"co_applicant_ethnicity_{x}_category"] = 'Puerto Rican'
        df.loc[df[f"co_applicant_ethnicity_{x}"] == 13, f"co_applicant_ethnicity_{x}_category"] = 'Cuban'
        df.loc[df[f"co_applicant_ethnicity_{x}"] == 14, f"co_applicant_ethnicity_{x}_category"] = 'Other Hispanic or Latino'
        df[f"co_applicant_ethnicity_{x}_category"] = df[f"co_applicant_ethnicity_{x}_category"].astype(str)

        df.loc[df[f"co_applicant_ethnicity_{x}"] == 3, f"co_applicant_ethnicity_{x}"] = np.nan
        df.loc[df[f"co_applicant_ethnicity_{x}"] == 4, f"co_applicant_ethnicity_{x}"] = np.nan
        df[f"co_applicant_ethnicity_{x}"] = pd.to_numeric(df[f"co_applicant_ethnicity_{x}"], errors="coerce").astype('Int64')
        df[f"applicant_race_{x}"] = pd.to_numeric(df[f"applicant_race_{x}"], errors="coerce").astype('Int64')
        df.loc[df[f"applicant_race_{x}"] == 1, f"applicant_race_{x}_category"] = 'American Indian or Alaska Native'
        df.loc[df[f"applicant_race_{x}"] == 2, f"applicant_race_{x}_category"] = 'Asian'
        df.loc[df[f"applicant_race_{x}"] == 21, f"applicant_race_{x}_category"] = 'Asian Indian'
        df.loc[df[f"applicant_race_{x}"] == 22, f"applicant_race_{x}_category"] = 'Chinese'
        df.loc[df[f"applicant_race_{x}"] == 23, f"applicant_race_{x}_category"] = 'Filipino'
        df.loc[df[f"applicant_race_{x}"] == 24, f"applicant_race_{x}_category"] = 'Japanese'
        df.loc[df[f"applicant_race_{x}"] == 25, f"applicant_race_{x}_category"] = 'Korean'
        df.loc[df[f"applicant_race_{x}"] == 26, f"applicant_race_{x}_category"] = 'Vietnamese'
        df.loc[df[f"applicant_race_{x}"] == 27, f"applicant_race_{x}_category"] = 'Other Asian'
        df.loc[df[f"applicant_race_{x}"] == 3, f"applicant_race_{x}_category"] = 'Black or African American'
        df.loc[df[f"applicant_race_{x}"] == 4, f"applicant_race_{x}_category"] = 'Native Hawaiian or Other Pacific Islander'
        df.loc[df[f"applicant_race_{x}"] == 41, f"applicant_race_{x}_category"] = 'Native Hawaiian'
        df.loc[df[f"applicant_race_{x}"] == 42, f"applicant_race_{x}_category"] = 'Guamanian or Chamorro'
        df.loc[df[f"applicant_race_{x}"] == 43, f"applicant_race_{x}_category"] = 'Samoan'
        df.loc[df[f"applicant_race_{x}"] == 44, f"applicant_race_{x}_category"] = 'Other Pacific Islander'
        df.loc[df[f"applicant_race_{x}"] == 5, f"applicant_race_{x}_category"] = 'White'
        df.loc[df[f"applicant_race_{x}"] == 6, f"applicant_race_{x}_category"] = np.nan
        df.loc[df[f"applicant_race_{x}"] == 7, f"applicant_race_{x}_category"] = np.nan
        df[f"applicant_race_{x}_category"] = df[f"applicant_race_{x}_category"].astype(str)
        
        df.loc[df[f"applicant_race_{x}"] == 1, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 2, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 21, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 22, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 23, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 24, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 25, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 26, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 27, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 3, f"applicant_race_{x}_category_grouped"] = 'Black'
        df.loc[df[f"applicant_race_{x}"] == 4, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 41, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 42, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 43, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 44, f"applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"applicant_race_{x}"] == 5, f"applicant_race_{x}_category_grouped"] = 'White'
        df.loc[df[f"applicant_race_{x}"] == 6, f"applicant_race_{x}_category_grouped"] = np.nan
        df.loc[df[f"applicant_race_{x}"] == 7, f"applicant_race_{x}_category_grouped"] = np.nan
        df.loc[df[f"applicant_race_{x}"] == 8, f"applicant_race_{x}_category_grouped"] = np.nan
        df[f"applicant_race_{x}_category_grouped"] = df[f"applicant_race_{x}_category_grouped"].astype(str)
        
        df.loc[df[f"applicant_race_{x}"] == 6, f"applicant_race_{x}"] = np.nan
        df.loc[df[f"applicant_race_{x}"] == 7, f"applicant_race_{x}"] = np.nan
        df.loc[df[f"applicant_race_{x}"] == 8, f"applicant_race_{x}"] = np.nan

        df[f"co_applicant_race_{x}"] = pd.to_numeric(df[f"co_applicant_race_{x}"], errors="coerce").astype('Int64')
        df.loc[df[f"co_applicant_race_{x}"] == 1, f"co_applicant_race_{x}_category"] = 'American Indian or Alaska Native'
        df.loc[df[f"co_applicant_race_{x}"] == 2, f"co_applicant_race_{x}_category"] = 'Asian'
        df.loc[df[f"co_applicant_race_{x}"] == 21, f"co_applicant_race_{x}_category"] = 'Asian Indian'
        df.loc[df[f"co_applicant_race_{x}"] == 22, f"co_applicant_race_{x}_category"] = 'Chinese'
        df.loc[df[f"co_applicant_race_{x}"] == 23, f"co_applicant_race_{x}_category"] = 'Filipino'
        df.loc[df[f"co_applicant_race_{x}"] == 24, f"co_applicant_race_{x}_category"] = 'Japanese'
        df.loc[df[f"co_applicant_race_{x}"] == 25, f"co_applicant_race_{x}_category"] = 'Korean'
        df.loc[df[f"co_applicant_race_{x}"] == 26, f"co_applicant_race_{x}_category"] = 'Vietnamese'
        df.loc[df[f"co_applicant_race_{x}"] == 27, f"co_applicant_race_{x}_category"] = 'Other Asian'
        df.loc[df[f"co_applicant_race_{x}"] == 3, f"co_applicant_race_{x}_category"] = 'Black or African American'
        df.loc[df[f"co_applicant_race_{x}"] == 4, f"co_applicant_race_{x}_category"] = 'Native Hawaiian or Other Pacific Islander'
        df.loc[df[f"co_applicant_race_{x}"] == 41, f"co_applicant_race_{x}_category"] = 'Native Hawaiian'
        df.loc[df[f"co_applicant_race_{x}"] == 42, f"co_applicant_race_{x}_category"] = 'Guamanian or Chamorro'
        df.loc[df[f"co_applicant_race_{x}"] == 43, f"co_applicant_race_{x}_category"] = 'Samoan'
        df.loc[df[f"co_applicant_race_{x}"] == 44, f"co_applicant_race_{x}_category"] = 'Other Pacific Islander'
        df.loc[df[f"co_applicant_race_{x}"] == 5, f"co_applicant_race_{x}_category"] = 'White'
        df.loc[df[f"co_applicant_race_{x}"] == 6, f"co_applicant_race_{x}_category"] = np.nan
        df.loc[df[f"co_applicant_race_{x}"] == 7, f"co_applicant_race_{x}_category"] = np.nan
        df.loc[df[f"co_applicant_race_{x}"] == 8, f"co_applicant_race_{x}_category"] = np.nan
        df[f"co_applicant_race_{x}_category"] = df[f"co_applicant_race_{x}_category"].astype(str)
        
        df.loc[df[f"co_applicant_race_{x}"] == 1, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 2, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 21, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 22, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 23, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 24, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 25, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 26, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 27, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 3, f"co_applicant_race_{x}_category_grouped"] = 'Black'
        df.loc[df[f"co_applicant_race_{x}"] == 4, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 41, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 42, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 43, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 44, f"co_applicant_race_{x}_category_grouped"] = 'AAPI'
        df.loc[df[f"co_applicant_race_{x}"] == 5, f"co_applicant_race_{x}_category_grouped"] = 'White'
        df.loc[df[f"co_applicant_race_{x}"] == 6, f"co_applicant_race_{x}_category_grouped"] = np.nan
        df.loc[df[f"co_applicant_race_{x}"] == 7, f"co_applicant_race_{x}_category_grouped"] = np.nan
        df.loc[df[f"co_applicant_race_{x}"] == 8, f"co_applicant_race_{x}_category_grouped"] = np.nan
        df[f"co_applicant_race_{x}_category_grouped"] = df[f"co_applicant_race_{x}_category_grouped"].astype(str)
        
        df.loc[df[f"co_applicant_race_{x}"] == 6, f"co_applicant_race_{x}"] = np.nan
        df.loc[df[f"co_applicant_race_{x}"] == 7, f"co_applicant_race_{x}"] = np.nan
        df.loc[df[f"co_applicant_race_{x}"] == 8, f"co_applicant_race_{x}"] = np.nan


    df.loc[df['applicant_ethnicity_observed'] == 1, 'applicant_ethnicity_observed_category'] = 'Ethnicity Collected on Basis of Observation'
    df.loc[df['applicant_ethnicity_observed'] == 2, 'applicant_ethnicity_observed_category'] = 'Ethnicity Not Based on Observation'
    df.loc[df['applicant_ethnicity_observed'] == 3, 'applicant_ethnicity_observed_category'] = np.nan
    df['applicant_ethnicity_observed_category'] = df['applicant_ethnicity_observed_category'].astype(str)

    df.loc[df['applicant_ethnicity_observed'] == 1, 'applicant_enthnicity_observed'] = 1
    df.loc[df['applicant_ethnicity_observed'] == 2, 'applicant_enthnicity_observed'] = 0
    df.loc[df['applicant_ethnicity_observed'] == 3, 'applicant_enthnicity_observed'] = np.nan

    df.loc[df['co_applicant_ethnicity_observed'] == 1, 'co_applicant_ethnicity_observed_category'] = 'Ethnicity Collected on Basis of Observation'
    df.loc[df['co_applicant_ethnicity_observed'] == 2, 'co_applicant_ethnicity_observed_category'] = 'Ethnicity Not Based on Observation'
    df.loc[df['co_applicant_ethnicity_observed'] == 3, 'co_applicant_ethnicity_observed_category'] = np.nan
    df.loc[df['co_applicant_ethnicity_observed'] == 4, 'co_applicant_ethnicity_observed_category'] = np.nan
    df['co_applicant_ethnicity_observed_category'] = df['co_applicant_ethnicity_observed_category'].astype(str)

    df.loc[df['co_applicant_ethnicity_observed'] == 1, 'co_applicant_enthnicity_observed'] = 1
    df.loc[df['co_applicant_ethnicity_observed'] == 2, 'co_applicant_enthnicity_observed'] = 0
    df.loc[df['co_applicant_ethnicity_observed'] == 3, 'co_applicant_enthnicity_observed'] = np.nan
    df.loc[df['co_applicant_ethnicity_observed'] == 4, 'co_applicant_enthnicity_observed'] = np.nan

    df.loc[df['applicant_race_observed'] == 1, 'applicant_race_observed_category'] = 'Collected on basis of visual observation'
    df.loc[df['applicant_race_observed'] == 2, 'applicant_race_observed_category'] = 'Not Collected on basis of visual observation'
    df.loc[df['applicant_race_observed'] == 3, 'applicant_race_observed_category'] = np.nan
    df['applicant_race_observed_category'] = df['applicant_race_observed_category'].astype(str)

    df.loc[df['applicant_race_observed'] == 1, 'applicant_race_observed'] = 1
    df.loc[df['applicant_race_observed'] == 2, 'applicant_race_observed'] = 0
    df.loc[df['applicant_race_observed'] == 3, 'applicant_race_observed'] = np.nan

    df.loc[df['co_applicant_race_observed'] == 1, 'co_applicant_race_observed_category'] = 'Collected on basis of visual observation'
    df.loc[df['co_applicant_race_observed'] == 2, 'co_applicant_race_observed_category'] = 'Not Collected on basis of visual observation'
    df.loc[df['co_applicant_race_observed'] == 3, 'co_applicant_race_observed_category'] = np.nan
    df.loc[df['co_applicant_race_observed'] == 4, 'co_applicant_race_observed_category'] = np.nan
    df['co_applicant_race_observed_category'] = df['co_applicant_race_observed_category'].astype(str)

    df.loc[df['co_applicant_race_observed'] == 1, 'co_applicant_race_observed'] = 1
    df.loc[df['co_applicant_race_observed'] == 2, 'co_applicant_race_observed'] = 0
    df.loc[df['co_applicant_race_observed'] == 3, 'co_applicant_race_observed'] = np.nan
    df.loc[df['co_applicant_race_observed'] == 4, 'co_applicant_race_observed'] = np.nan

    df.loc[df['applicant_sex'] == 1, 'applicant_sex_category'] = 'Male'
    df.loc[df['applicant_sex'] == 2, 'applicant_sex_category'] = 'Female'
    df.loc[df['applicant_sex'] == 3, 'applicant_sex_category'] = np.nan
    df.loc[df['applicant_sex'] == 4, 'applicant_sex_category'] = np.nan
    df.loc[df['applicant_sex'] == 6, 'applicant_sex_category'] = 'Male and Female'
    df['applicant_sex_category'] = df['applicant_sex_category'].astype(str)

    df.loc[df['applicant_sex'] == 1, 'applicant_sex'] = 1
    df.loc[df['applicant_sex'] == 2, 'applicant_sex'] = 2
    df.loc[df['applicant_sex'] == 3, 'applicant_sex'] = np.nan
    df.loc[df['applicant_sex'] == 4, 'applicant_sex'] = np.nan
    df.loc[df['applicant_sex'] == 6, 'applicant_sex'] = 3

    df.loc[df['co_applicant_sex'] == 1, 'co_applicant_sex_category'] = 'Male'
    df.loc[df['co_applicant_sex'] == 2, 'co_applicant_sex_category'] = 'Female'
    df.loc[df['co_applicant_sex'] == 3, 'co_applicant_sex_category'] = np.nan
    df.loc[df['co_applicant_sex'] == 4, 'co_applicant_sex_category'] = np.nan
    df.loc[df['co_applicant_sex'] == 5, 'co_applicant_sex_category'] = np.nan
    df.loc[df['co_applicant_sex'] == 6, 'co_applicant_sex_category'] = 'Male and Female'
    df['co_applicant_sex_category'] = df['co_applicant_sex_category'].astype(str)

    df.loc[df['co_applicant_sex'] == 1, 'co_applicant_sex'] = 1
    df.loc[df['co_applicant_sex'] == 2, 'co_applicant_sex'] = 2
    df.loc[df['co_applicant_sex'] == 3, 'co_applicant_sex'] = np.nan
    df.loc[df['co_applicant_sex'] == 4, 'co_applicant_sex'] = np.nan
    df.loc[df['co_applicant_sex'] == 5, 'co_applicant_sex'] = np.nan
    df.loc[df['co_applicant_sex'] == 6, 'co_applicant_sex'] = 3

    df.loc[df['applicant_sex_observed'] == 1, 'applicant_sex_observed_category'] = 'Collected on basis of visual observation'
    df.loc[df['applicant_sex_observed'] == 2, 'applicant_sex_observed_category'] = 'Not Collected on basis of visual observation'
    df.loc[df['applicant_sex_observed'] == 3, 'applicant_sex_observed_category'] = np.nan
    df['applicant_sex_observed_category'] = df['applicant_sex_observed_category'].astype(str)


    df.loc[df['applicant_sex_observed'] == 1, 'applicant_sex_observed'] = 1
    df.loc[df['applicant_sex_observed'] == 2, 'applicant_sex_observed'] = 0
    df.loc[df['applicant_sex_observed'] == 3, 'applicant_sex_observed'] = np.nan

    df.loc[df['co_applicant_sex_observed'] == 1, 'co_applicant_sex_observed_category'] = 'Collected on basis of visual observation'
    df.loc[df['co_applicant_sex_observed'] == 2, 'co_applicant_sex_observed_category'] = 'Not Collected on basis of visual observation'
    df.loc[df['co_applicant_sex_observed'] == 3, 'co_applicant_sex_observed_category'] = np.nan
    df.loc[df['co_applicant_sex_observed'] == 4, 'co_applicant_sex_observed_category'] = np.nan
    df['co_applicant_sex_observed_category'] = df['co_applicant_sex_observed_category'].astype(str)


    df.loc[df['co_applicant_sex_observed'] == 1, 'co_applicant_sex_observed'] = 1
    df.loc[df['co_applicant_sex_observed'] == 2, 'co_applicant_sex_observed'] = 0
    df.loc[df['co_applicant_sex_observed'] == 3, 'co_applicant_sex_observed'] = np.nan
    df.loc[df['co_applicant_sex_observed'] == 4, 'co_applicant_sex_observed'] = np.nan

    df.loc[df['applicant_age'].astype('str') == '8888'] = np.nan
    df.loc[df['applicant_age'].astype('str') == '8888.0'] = np.nan

    df.loc[df['co_applicant_age'].astype('str') == '8888'] = np.nan
    df.loc[df['co_applicant_age'].astype('str') == '8888.0'] = np.nan
    df.loc[df['co_applicant_age'].astype('str') == '9999'] = np.nan
    df.loc[df['co_applicant_age'].astype('str') == '9999.0'] = np.nan

    df.loc[df['applicant_age_above_62'] == 'Yes', 'applicant_age_above_62'] = 1
    df.loc[df['applicant_age_above_62'] == 'No', 'applicant_age_above_62'] = 0
    df[f"applicant_age_above_62"] = pd.to_numeric(df[f"applicant_age_above_62"], errors="coerce").astype('Int64')

    df.loc[df['co_applicant_age_above_62'] == 'Yes', 'co_applicant_age_above_62'] = 1
    df.loc[df['co_applicant_age_above_62'] == 'No', 'co_applicant_age_above_62'] = 0
    df[f"coapplicant_age_above_62"] = pd.to_numeric(df[f"co_applicant_age_above_62"], errors="coerce").astype('Int64')

    df[f"submission_of_application"] = pd.to_numeric(df[f"submission_of_application"], errors="coerce").astype('Int64')
    df.loc[df['submission_of_application'] == 1, 'submission_of_application_category'] = 'Submitted directly to institution'
    df.loc[df['submission_of_application'] == 2, 'submission_of_application_category'] = 'Not Submitted to institution'
    df.loc[df['submission_of_application'] == 3, 'submission_of_application_category'] = np.nan
    df.loc[df['submission_of_application'] == 1111, 'submission_of_application_category'] = 'Exempt'
    df['submission_of_application_category'] = df['submission_of_application_category'].astype(str)

    df.loc[df['submission_of_application'] == 1, 'submission_of_application'] = 1
    df.loc[df['submission_of_application'] == 2, 'submission_of_application'] = 0
    df.loc[df['submission_of_application'] == 3, 'submission_of_application'] = np.nan
    df.loc[df['submission_of_application'] == 11111, 'submission_of_application'] = np.nan

    df[f"initially_payable_to_institution"] = pd.to_numeric(df[f"initially_payable_to_institution"], errors="coerce").astype('Int64')
    df.loc[df['initially_payable_to_institution'] == 1, 'initially_payable_to_institution_category'] = 'Initially Payable to Institution'
    df.loc[df['initially_payable_to_institution'] == 2, 'initially_payable_to_institution_category'] = 'Not Initially Payable to Institution'
    df.loc[df['initially_payable_to_institution'] == 3, 'initially_payable_to_institution_category'] = np.nan
    df.loc[df['initially_payable_to_institution'] == 1111, 'initially_payable_to_institution_category'] = 'Exempt'
    df['initially_payable_to_institution_category'] = df['initially_payable_to_institution_category'].astype(str)

    df.loc[df['initially_payable_to_institution'] == 1, 'initially_payable_to_institution'] = 1
    df.loc[df['initially_payable_to_institution'] == 2, 'initially_payable_to_institution'] = 0
    df.loc[df['initially_payable_to_institution'] == 3, 'initially_payable_to_institution'] = np.nan
    df.loc[df['initially_payable_to_institution'] == 1111, 'initially_payable_to_institution'] = np.nan

    for x in range(1, 6): 
        df[f"aus_{x}"] = pd.to_numeric(df[f"aus_{x}"], errors='coerce').astype('Int64')
        df.loc[df[f"aus_{x}"] == 1, f"aus_{x}_category"] = 'Desktop Underwriter'
        df.loc[df[f"aus_{x}"] == 2, f"aus_{x}_category"] = 'Loan Prospector or Loan Product Advisor'
        df.loc[df[f"aus_{x}"] == 3, f"aus_{x}_category"] = 'Technology Open to Approved Lenders'
        df.loc[df[f"aus_{x}"] == 4, f"aus_{x}_category"] = 'Guaranteed Underwriting System'
        df.loc[df[f"aus_{x}"] == 5, f"aus_{x}_category"] = 'Other'
        df.loc[df[f"aus_{x}"] == 6, f"aus_{x}_category"] = np.nan
        df.loc[df[f"aus_{x}"] == 7, f"aus_{x}_category"] = 'Internal Proprietary System'
        df.loc[df[f"aus_{x}"] == 1111, f"aus_{x}_category"] = 'Exempt'
        df[f"aus_{x}_category"] = df[f"aus_{x}_category"].astype(str)
        
        df.loc[df[f"aus_{x}"] == 6, f"aus_{x}"] = np.nan
        df.loc[df[f"aus_{x}"] == 1111, f"aus_{x}"] = np.nan

    for x in range(1, 5): 
        df[f"denial_reason_{x}"] = pd.to_numeric(df[f"denial_reason_{x}"], errors='coerce').astype('Int64')
        df.loc[df[f"denial_reason_{x}"] == 1, f"denial_reason_{x}_category"] = 'Debt to Income Ratio'
        df.loc[df[f"denial_reason_{x}"] == 2, f"denial_reason_{x}_category"] = 'Employment History'
        df.loc[df[f"denial_reason_{x}"] == 3, f"denial_reason_{x}_category"] = 'Credit History'
        df.loc[df[f"denial_reason_{x}"] == 4, f"denial_reason_{x}_category"] = 'Collateral'
        df.loc[df[f"denial_reason_{x}"] == 5, f"denial_reason_{x}_category"] = 'Insufficient cash - downpayment or closing costs'
        df.loc[df[f"denial_reason_{x}"] == 6, f"denial_reason_{x}_category"] = 'Unverifiable Information'
        df.loc[df[f"denial_reason_{x}"] == 7, f"denial_reason_{x}_category"] = 'Credit application incomplete'
        df.loc[df[f"denial_reason_{x}"] == 8, f"denial_reason_{x}_category"] = 'Mortgage insurance denied'
        df.loc[df[f"denial_reason_{x}"] == 9, f"denial_reason_{x}_category"] = 'Other'
        df.loc[df[f"denial_reason_{x}"] == 10, f"denial_reason_{x}_category"] = np.nan
        df[f"denial_reason_{x}_category"] = df[f"denial_reason_{x}_category"].astype(str)
        
        df.loc[df[f"denial_reason_{x}"] == 10, f"denial_reason_{x}"] = np.nan

    df = enforce_column_types(df)
    validate_schema(df)

    return df
