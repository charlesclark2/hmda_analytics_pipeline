import argparse
import boto3
import pandas as pd
import io
import s3fs
import numpy as np 
import pyarrow as pa 
import pyarrow.parquet as pq


def send_dataframe_to_s3_csv(df, bucket, key, profile=None):
    # Set up AWS session
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client('s3')

    # Create in-memory CSV buffer
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"✅ Uploaded to s3://{bucket}/{key}", flush=True)


### Utility Transformation Functions ###
def categorize_purchaser(code):
    if code in [1, 3]:
        return "GSE"
    elif code == 2:
        return "Government"
    elif code == 0:
        return "Portfolio"
    elif code in [5, 7]:
        return "Private"
    elif code == 6:
        return "Depository"
    elif code == 8:
        return "Affiliate"
    else:
        return "Other"
    

def categorize_loan_type(code):
    if code == 1:
        return "Conventional"
    elif code == 2:
        return "FHA"
    elif code == 3:
        return "VA"
    elif code == 4:
        return "USDA"
    else:
        return "Other"
    

def categorize_loan_purpose(code): 
    if code == 1: 
        return 'Home Purchase'
    elif code == 2: 
        return 'Home Improvement'
    elif code in [31, 32]: 
        return 'Refinancing'
    elif code == 5: 
        return np.nan
    else: 
        return 'Other'


def categorize_lien_status(code): 
    if code == 1: 
        return "First Lien"
    elif code == 2: 
        return "Subordinate Lien"
    else: 
        return "Other"
    

def total_units_bucketed(code): 
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
        elif val >= 20 and val < 30: 
            return '20%-<30%'
        elif val >= 30 and val < 36: 
            return '30%-<36%'
        elif val >= 36 and val < 40: 
            return '36%-<40%'
        elif val >= 40 and val < 46: 
            return '40%-<46%'
        elif val >= 46 and val < 50: 
            return '46%-<50%'
        elif val >= 50 and val <= 60: 
            return '50%-60%'
        elif val > 60: 
            return '>60%'
        else: 
            return x
    except: 
        return x 
    

    

def main(source_bucket, target_bucket, raw_prefix, clean_prefix, clean_file_base, profile=None, chunksize=250_000): 
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client('s3')

    fs = s3fs.S3FileSystem( profile=profile)
    print(f"Getting files from {raw_prefix}", flush=True)
    parquet_files = fs.glob(f"{source_bucket}/{raw_prefix}*.parquet")
    for file in parquet_files: 
        root_file_part = file.split('/')[-1].split('_')[-1].replace('.parquet', '')
        print(f"Processing {file}", flush=True)
        df = pd.read_parquet(f"s3://{file}", engine='pyarrow', filesystem=fs)

        df_columns = df.columns.tolist()
        print(df.columns.tolist())

        # drop duplicates
        print("Dropping duplicates", flush=True)
        df.drop_duplicates(inplace=True)

        print(f"Converting columns")
        df.loc[df['state_code'] == 'nan', 'state_code'] = np.nan

        df['county_code'] = df['county_code'].astype('Int64')

        df.loc[df['conforming_loan_limit'] == 'C', 'conforming_loan_limit'] = 1
        df.loc[df['conforming_loan_limit'] == 'NC', 'conforming_loan_limit'] = 0
        df['conforming_loan_limit'] = pd.to_numeric(df['conforming_loan_limit'], errors='coerce').astype('Int64')

        df.loc[df['action_taken'] == 1, 'action_taken_category'] = 'Origination'
        df.loc[df['action_taken'] == 3, 'action_taken_category'] = 'Denied'
        df.loc[df['action_taken'] == 4, 'action_taken_category'] = 'Fallout'
        df.loc[df['action_taken'] == 5, 'action_taken_category'] = 'Fallout'
        df.loc[df['action_taken'] == 2, 'action_taken_category'] = 'Fallout'
        df.loc[df['action_taken'] == 6, 'action_taken_category'] = 'Purchased - Secondary Market'
        df.loc[df['action_taken'] == 7, 'action_taken_category'] = 'Preapproval - Denial'
        df.loc[df['action_taken'] == 8, 'action_taken_category'] = 'Preapproval - Approved not accepted'

        df["purchaser_category"] = df["purchaser_type"].apply(categorize_purchaser)

        df.loc[df['preapproval'] == 2, 'preapproval'] = 0

        df["loan_type_category"] = df["loan_type"].apply(categorize_loan_type)

        df['loan_purpose_category'] = df['loan_purpose'].apply(categorize_loan_purpose)

        df['lien_status_category'] = df['lien_status'].apply(categorize_lien_status)

        df.loc[df['reverse_mortgage'] == 1, 'reverse_mortgage_category'] = 'Reverse Mortgage'
        df.loc[df['reverse_mortgage'] == 2, 'reverse_mortgage_category'] = 'Not Reverse Mortgage'
        df.loc[df['reverse_mortgage'] == 1111, 'reverse_mortgage_category'] = 'Exempt'

        df.loc[df['reverse_mortgage'] == 1, 'reverse_mortgage'] = 1
        df.loc[df['reverse_mortgage'] == 2, 'reverse_mortgage'] = 0
        df.loc[df['reverse_mortgage'] == 1111, 'reverse_mortgage'] = np.nan

        df.loc[df['open_end_line_of_credit'] == 1, 'open_end_line_of_credit_category'] = 'Open Line of Credit'
        df.loc[df['open_end_line_of_credit'] == 2, 'open_end_line_of_credit_category'] = 'Not Open Line of Credit'
        df.loc[df['open_end_line_of_credit'] == 1111, 'open_end_line_of_credit_category'] = 'Exempt'

        df.loc[df['open_end_line_of_credit'] == 1, 'open_end_line_of_credit'] = 1
        df.loc[df['open_end_line_of_credit'] == 2, 'open_end_line_of_credit'] = 0
        df.loc[df['open_end_line_of_credit'] == 1111, 'open_end_line_of_credit'] = np.nan

        df.loc[df['business_or_commercial_purpose'] == 1, 'business_or_commercial_purpose_category'] = 'Business Purpose'
        df.loc[df['business_or_commercial_purpose'] == 2, 'business_or_commercial_purpose_category'] = 'Not Business Purpose'
        df.loc[df['business_or_commercial_purpose'] == 1111, 'business_or_commercial_purpose_category'] = 'Exempt'

        df.loc[df['business_or_commercial_purpose'] == 1, 'business_or_commercial_purpose'] = 1
        df.loc[df['business_or_commercial_purpose'] == 2, 'business_or_commercial_purpose'] = 0
        df.loc[df['business_or_commercial_purpose'] == 1111, 'business_or_commercial_purpose'] = np.nan

        df = df.reset_index(drop=True)
        if 'loan_to_value_ratio' in df_columns: 
            df['combined_loan_to_value_ratio'] = pd.to_numeric(df['combined_loan_to_value_ratio'], errors='coerce')
            df['effective_ltv'] = df['loan_to_value_ratio']
            df.loc[df['effective_ltv'].isna(), 'effective_ltv'] = df['combined_loan_to_value_ratio']
        elif 'combined_loan_to_value_ratio' in df_columns: 
            df['combined_loan_to_value_ratio'] = pd.to_numeric(df['combined_loan_to_value_ratio'], errors='coerce')
            df['effective_ltv'] = np.nan
            df.loc[df['effective_ltv'].isna(), 'effective_ltv'] = df['combined_loan_to_value_ratio']
        else: 
            df['effective_ltv'] = np.nan


        df['interest_rate'] = pd.to_numeric(df['interest_rate'], errors='coerce')

        df['rate_spread'] = pd.to_numeric(df['rate_spread'], errors='coerce')

        df.loc[df['hoepa_status'] == 1, 'hopea_status_category'] = 'High Cost Mortgage'
        df.loc[df['hoepa_status'] == 2, 'hopea_status_category'] = 'Not a High Cost Mortgage'
        df.loc[df['hoepa_status'] == 3, 'hopea_status_category'] = np.nan

        df.loc[df['hoepa_status'] == 1, 'hopea_status'] = 1
        df.loc[df['hoepa_status'] == 2, 'hopea_status'] = 0
        df.loc[df['hoepa_status'] == 3, 'hopea_status'] = np.nan   

        

        df.loc[df['total_loan_costs'].isin(['nan', 'Exempt']), 'total_loan_costs'] = np.nan
        df['total_loan_costs'] = pd.to_numeric(df['total_loan_costs'], errors='coerce')

        df.loc[df['total_points_and_fees'].isin(['nan', 'Exempt']), 'total_points_and_fees'] = np.nan
        df['total_points_and_fees'] = pd.to_numeric(df['total_points_and_fees'], errors='coerce')

        df.loc[df['origination_charges'].isin(['nan', 'Exempt']), 'origination_charges'] = np.nan
        df['origination_charges'] = pd.to_numeric(df['origination_charges'], errors='coerce')

        # discount_points
        df.loc[df['discount_points'].isin(['nan', 'Exempt']), 'discount_points'] = np.nan
        df['discount_points'] = pd.to_numeric(df['discount_points'], errors='coerce')

        # lender_credits
        df.loc[df['lender_credits'].isin(['nan', 'Exempt']), 'lender_credits'] = np.nan
        df['lender_credits'] = pd.to_numeric(df['lender_credits'], errors='coerce')

        # loan_term
        df.loc[df['loan_term'].isin(['nan', 'Exempt']), 'loan_term'] = np.nan
        df['loan_term'] = pd.to_numeric(df['loan_term'], errors='coerce').astype('Int64')

        # prepayment_penalty_term
        df.loc[df['prepayment_penalty_term'].isin(['nan', 'Exempt']), 'prepayment_penalty_term'] = np.nan
        df['prepayment_penalty_term'] = pd.to_numeric(df['prepayment_penalty_term'], errors='coerce').astype('Int64')

        # intro_rate_period
        df.loc[df['intro_rate_period'].isin(['nan', 'Exempt']), 'intro_rate_period'] = np.nan
        df['intro_rate_period'] = pd.to_numeric(df['intro_rate_period'], errors='coerce').astype('Int64')

        # negative_amortization
        df.loc[df['negative_amortization'] == 1, 'negative_amortization_category'] = 'Negative Amortization Features'
        df.loc[df['negative_amortization'] == 2, 'negative_amortization_category'] = 'No Negative Amortization Features'
        df.loc[df['negative_amortization'] == 1111, 'negative_amortization_category'] = np.nan

        df.loc[df['negative_amortization'] == 1, 'negative_amortization'] = 1
        df.loc[df['negative_amortization'] == 2, 'negative_amortization'] = 0
        df.loc[df['negative_amortization'] == 1111, 'negative_amortization'] = np.nan

        # interest_only_payment
        df.loc[df['interest_only_payment'] == 1, 'interest_only_payment_category'] = 'Interest-Only Payments'
        df.loc[df['interest_only_payment'] == 2, 'interest_only_payment_category'] = 'No Interest-Only Payments'
        df.loc[df['interest_only_payment'] == 1111, 'interest_only_payment_category'] = np.nan

        df.loc[df['interest_only_payment'] == 1, 'interest_only_payment'] = 1
        df.loc[df['interest_only_payment'] == 2, 'interest_only_payment'] = 0
        df.loc[df['interest_only_payment'] == 1111, 'interest_only_payment'] = np.nan

        # balloon_payment
        df.loc[df['balloon_payment'] == 1, 'balloon_payment_category'] = 'Balloon Payment'
        df.loc[df['balloon_payment'] == 2, 'balloon_payment_category'] = 'No Balloon Payment'
        df.loc[df['balloon_payment'] == 1111, 'balloon_payment_category'] = np.nan

        df.loc[df['balloon_payment'] == 1, 'balloon_payment'] = 1
        df.loc[df['balloon_payment'] == 2, 'balloon_payment'] = 0
        df.loc[df['balloon_payment'] == 1111, 'balloon_payment'] = np.nan

        # other_nonamortizing_features
        df.loc[df['other_nonamortizing_features'] == 1, 'other_nonamortizing_features_category'] = 'Other non-fully amortizing features'
        df.loc[df['other_nonamortizing_features'] == 2, 'other_nonamortizing_features_category'] = 'No other non-fully amortizing features'
        df.loc[df['other_nonamortizing_features'] == 1111, 'other_nonamortizing_features_category'] = np.nan

        df.loc[df['other_nonamortizing_features'] == 1, 'other_nonamortizing_features'] = 1
        df.loc[df['other_nonamortizing_features'] == 2, 'other_nonamortizing_features'] = 0
        df.loc[df['other_nonamortizing_features'] == 1111, 'other_nonamortizing_features'] = np.nan

        # property_value
        df.loc[df['property_value'].isin(['nan', 'Exempt']), 'property_value'] = np.nan
        df['property_value'] = pd.to_numeric(df['property_value'], errors='coerce').astype('Int64')

        # construction_method
        df.loc[df['construction_method'] == 1, 'construction_method_category'] = 'Site-built'
        df.loc[df['construction_method'] == 2, 'construction_method_category'] = 'Manufactured Home'

        # occupancy_type
        df.loc[df['occupancy_type'] == 1, 'occupancy_type_category'] = 'Principal Residence'
        df.loc[df['occupancy_type'] == 2, 'occupancy_type_category'] = 'Second Residence'
        df.loc[df['occupancy_type'] == 3, 'occupancy_type_category'] = 'Investment Property'

        # manufactured_home_secured_property_type
        df.loc[df['manufactured_home_secured_property_type'] == 1, 'manufactured_home_secured_property_type_category'] = 'Manufactured home and land'
        df.loc[df['manufactured_home_secured_property_type'] == 2, 'manufactured_home_secured_property_type_category'] = 'Manufactured Home - Not Land'
        df.loc[df['manufactured_home_secured_property_type'] == 3, 'manufactured_home_secured_property_type_category'] = np.nan
        df.loc[df['manufactured_home_secured_property_type'] == 1111, 'manufactured_home_secured_property_type_category'] = np.nan

        df.loc[df['manufactured_home_secured_property_type'] == 3, 'manufactured_home_secured_property_type'] = np.nan
        df.loc[df['manufactured_home_secured_property_type'] == 1111, 'manufactured_home_secured_property_type'] = np.nan

        # manufactured_home_land_property_interest
        df.loc[df['manufactured_home_land_property_interest'] == 1, 'manufactured_home_land_property_interest_category'] = 'Direct Ownership'
        df.loc[df['manufactured_home_land_property_interest'] == 2, 'manufactured_home_land_property_interest_category'] = 'Indirect Ownership'
        df.loc[df['manufactured_home_land_property_interest'] == 3, 'manufactured_home_land_property_interest_category'] = 'Paid Leasehold'
        df.loc[df['manufactured_home_land_property_interest'] == 4, 'manufactured_home_land_property_interest_category'] = 'Unpaid Leasehold'
        df.loc[df['manufactured_home_land_property_interest'] == 5, 'manufactured_home_land_property_interest_category'] = np.nan
        df.loc[df['manufactured_home_land_property_interest'] == 1111, 'manufactured_home_land_property_interest_category'] = np.nan

        df.loc[df['manufactured_home_land_property_interest'] == 5, 'manufactured_home_land_property_interest'] = np.nan
        df.loc[df['manufactured_home_land_property_interest'] == 1111, 'manufactured_home_land_property_interest'] = np.nan

        # total_units
        df.loc[df['total_units'].isin([1, 1.0, '1']), 'total_units'] = '1'
        df.loc[df['total_units'].isin([2, 2.0, '2']), 'total_units'] = '2'
        df.loc[df['total_units'].isin([3, 4.0, '3']), 'total_units'] = '3'
        df.loc[df['total_units'].isin([4, 4.0, '4']), 'total_units'] = '4'

        df['total_units_bucketed'] = df['total_units'].apply(total_units_bucketed)

        # multifamily_affordable_units
        df["multifamily_affordable_units"] = pd.to_numeric(df["multifamily_affordable_units"], errors="coerce").astype('Int64')
        df["multifamily_affordable_units"] = df["multifamily_affordable_units"].fillna(0)

        # income
        df['income'] = pd.to_numeric(df['income'], errors='coerce').astype('Int64')
        df['income'] = df['income'] * 1000

        df['debt_to_income_ratio'] = df['debt_to_income_ratio'].apply(transform_dti)

        # applicant_credit_score_type
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

        df.loc[df['applicant_credit_score_type'] == 9, 'applicant_credit_score_type'] = np.nan
        df.loc[df['applicant_credit_score_type'] == 1111, 'applicant_credit_score_type'] = np.nan

        # co_applicant_credit_score_type
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

        df.loc[df['co_applicant_credit_score_type'] == 9, 'co_applicant_credit_score_type'] = np.nan
        df.loc[df['co_applicant_credit_score_type'] == 1111, 'co_applicant_credit_score_type'] = np.nan

        # applicant_ethnicity_X, co_applicant_ethnicity_X, applicant_race_X, and co_applicant_race_X
        for x in range(1, 6): 
            df.loc[df[f"applicant_ethnicity_{x}"] == 1, f"applicant_ethnicity_{x}_category"] = 'Hispanic or Latino'
            df.loc[df[f"applicant_ethnicity_{x}"] == 2, f"applicant_ethnicity_{x}_category"] = 'Not Hispanic or Latino'
            df.loc[df[f"applicant_ethnicity_{x}"] == 3, f"applicant_ethnicity_{x}_category"] = np.nan
            df.loc[df[f"applicant_ethnicity_{x}"] == 4, f"applicant_ethnicity_{x}_category"] = np.nan
            df.loc[df[f"applicant_ethnicity_{x}"] == 11, f"applicant_ethnicity_{x}_category"] = 'Mexican'
            df.loc[df[f"applicant_ethnicity_{x}"] == 12, f"applicant_ethnicity_{x}_category"] = 'Puerto Rican'
            df.loc[df[f"applicant_ethnicity_{x}"] == 13, f"applicant_ethnicity_{x}_category"] = 'Cuban'
            df.loc[df[f"applicant_ethnicity_{x}"] == 14, f"applicant_ethnicity_{x}_category"] = 'Other Hispanic or Latino'
            
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
            
            df.loc[df[f"co_applicant_race_{x}"] == 6, f"co_applicant_race_{x}"] = np.nan
            df.loc[df[f"co_applicant_race_{x}"] == 7, f"co_applicant_race_{x}"] = np.nan
            df.loc[df[f"co_applicant_race_{x}"] == 8, f"co_applicant_race_{x}"] = np.nan
        
        # create no co applicant column
        df['co_applicant_present'] = (df['co_applicant_ethnicity_1'] != 5).astype('Int64')

        # applicant_ethnicity_observed
        df.loc[df['applicant_ethnicity_observed'] == 1, 'applicant_ethnicity_observed_category'] = 'Ethnicity Collected on Basis of Observation'
        df.loc[df['applicant_ethnicity_observed'] == 2, 'applicant_ethnicity_observed_category'] = 'Ethnicity Not Based on Observation'
        df.loc[df['applicant_ethnicity_observed'] == 3, 'applicant_ethnicity_observed_category'] = np.nan

        df.loc[df['applicant_ethnicity_observed'] == 1, 'applicant_enthnicity_observed'] = 1
        df.loc[df['applicant_ethnicity_observed'] == 2, 'applicant_enthnicity_observed'] = 0
        df.loc[df['applicant_ethnicity_observed'] == 3, 'applicant_enthnicity_observed'] = np.nan

        # co applicant_ethnicity_observed
        df.loc[df['co_applicant_ethnicity_observed'] == 1, 'co_applicant_ethnicity_observed_category'] = 'Ethnicity Collected on Basis of Observation'
        df.loc[df['co_applicant_ethnicity_observed'] == 2, 'co_applicant_ethnicity_observed_category'] = 'Ethnicity Not Based on Observation'
        df.loc[df['co_applicant_ethnicity_observed'] == 3, 'co_applicant_ethnicity_observed_category'] = np.nan
        df.loc[df['co_applicant_ethnicity_observed'] == 4, 'co_applicant_ethnicity_observed_category'] = np.nan

        df.loc[df['co_applicant_ethnicity_observed'] == 1, 'co_applicant_enthnicity_observed'] = 1
        df.loc[df['co_applicant_ethnicity_observed'] == 2, 'co_applicant_enthnicity_observed'] = 0
        df.loc[df['co_applicant_ethnicity_observed'] == 3, 'co_applicant_enthnicity_observed'] = np.nan
        df.loc[df['co_applicant_ethnicity_observed'] == 4, 'co_applicant_enthnicity_observed'] = np.nan

        # applicant_race_observed
        df.loc[df['applicant_race_observed'] == 1, 'applicant_race_observed_category'] = 'Collected on basis of visual observation'
        df.loc[df['applicant_race_observed'] == 2, 'applicant_race_observed_category'] = 'Not Collected on basis of visual observation'
        df.loc[df['applicant_race_observed'] == 3, 'applicant_race_observed_category'] = np.nan

        df.loc[df['applicant_race_observed'] == 1, 'applicant_race_observed'] = 1
        df.loc[df['applicant_race_observed'] == 2, 'applicant_race_observed'] = 0
        df.loc[df['applicant_race_observed'] == 3, 'applicant_race_observed'] = np.nan

        # applicant_race_observed
        df.loc[df['co_applicant_race_observed'] == 1, 'co_applicant_race_observed_category'] = 'Collected on basis of visual observation'
        df.loc[df['co_applicant_race_observed'] == 2, 'co_applicant_race_observed_category'] = 'Not Collected on basis of visual observation'
        df.loc[df['co_applicant_race_observed'] == 3, 'co_applicant_race_observed_category'] = np.nan
        df.loc[df['co_applicant_race_observed'] == 4, 'co_applicant_race_observed_category'] = np.nan

        df.loc[df['co_applicant_race_observed'] == 1, 'co_applicant_race_observed'] = 1
        df.loc[df['co_applicant_race_observed'] == 2, 'co_applicant_race_observed'] = 0
        df.loc[df['co_applicant_race_observed'] == 3, 'co_applicant_race_observed'] = np.nan
        df.loc[df['co_applicant_race_observed'] == 4, 'co_applicant_race_observed'] = np.nan

        # applicant_sex
        df.loc[df['applicant_sex'] == 1, 'applicant_sex_category'] = 'Male'
        df.loc[df['applicant_sex'] == 2, 'applicant_sex_category'] = 'Female'
        df.loc[df['applicant_sex'] == 3, 'applicant_sex_category'] = np.nan
        df.loc[df['applicant_sex'] == 4, 'applicant_sex_category'] = np.nan
        df.loc[df['applicant_sex'] == 6, 'applicant_sex_category'] = 'Male and Female'

        df.loc[df['applicant_sex'] == 1, 'applicant_sex'] = 1
        df.loc[df['applicant_sex'] == 2, 'applicant_sex'] = 2
        df.loc[df['applicant_sex'] == 3, 'applicant_sex'] = np.nan
        df.loc[df['applicant_sex'] == 4, 'applicant_sex'] = np.nan
        df.loc[df['applicant_sex'] == 6, 'applicant_sex'] = 3

        # co_applicant_sex
        df.loc[df['co_applicant_sex'] == 1, 'co_applicant_sex_category'] = 'Male'
        df.loc[df['co_applicant_sex'] == 2, 'co_applicant_sex_category'] = 'Female'
        df.loc[df['co_applicant_sex'] == 3, 'co_applicant_sex_category'] = np.nan
        df.loc[df['co_applicant_sex'] == 4, 'co_applicant_sex_category'] = np.nan
        df.loc[df['co_applicant_sex'] == 5, 'co_applicant_sex_category'] = np.nan
        df.loc[df['co_applicant_sex'] == 6, 'co_applicant_sex_category'] = 'Male and Female'

        df.loc[df['co_applicant_sex'] == 1, 'co_applicant_sex'] = 1
        df.loc[df['co_applicant_sex'] == 2, 'co_applicant_sex'] = 2
        df.loc[df['co_applicant_sex'] == 3, 'co_applicant_sex'] = np.nan
        df.loc[df['co_applicant_sex'] == 4, 'co_applicant_sex'] = np.nan
        df.loc[df['co_applicant_sex'] == 5, 'co_applicant_sex'] = np.nan
        df.loc[df['co_applicant_sex'] == 6, 'co_applicant_sex'] = 3

        # handling framentation in case it happens
        df = df.copy()
    # applicant_sex_observed
        df.loc[df['applicant_sex_observed'] == 1, 'applicant_sex_observed_category'] = 'Collected on basis of visual observation'
        df.loc[df['applicant_sex_observed'] == 2, 'applicant_sex_observed_category'] = 'Not Collected on basis of visual observation'
        df.loc[df['applicant_sex_observed'] == 3, 'applicant_sex_observed_category'] = np.nan


        df.loc[df['applicant_sex_observed'] == 1, 'applicant_sex_observed'] = 1
        df.loc[df['applicant_sex_observed'] == 2, 'applicant_sex_observed'] = 0
        df.loc[df['applicant_sex_observed'] == 3, 'applicant_sex_observed'] = np.nan

        # co_applicant_sex_observed
        df.loc[df['co_applicant_sex_observed'] == 1, 'co_applicant_sex_observed_category'] = 'Collected on basis of visual observation'
        df.loc[df['co_applicant_sex_observed'] == 2, 'co_applicant_sex_observed_category'] = 'Not Collected on basis of visual observation'
        df.loc[df['co_applicant_sex_observed'] == 3, 'co_applicant_sex_observed_category'] = np.nan
        df.loc[df['co_applicant_sex_observed'] == 4, 'co_applicant_sex_observed_category'] = np.nan


        df.loc[df['co_applicant_sex_observed'] == 1, 'co_applicant_sex_observed'] = 1
        df.loc[df['co_applicant_sex_observed'] == 2, 'co_applicant_sex_observed'] = 0
        df.loc[df['co_applicant_sex_observed'] == 3, 'co_applicant_sex_observed'] = np.nan
        df.loc[df['co_applicant_sex_observed'] == 4, 'co_applicant_sex_observed'] = np.nan

        # applicant_age
        df.loc[df['applicant_age'].astype('str') == '8888'] = np.nan
        df.loc[df['applicant_age'].astype('str') == '8888.0'] = np.nan
            
        # co_applicant_age
        df.loc[df['co_applicant_age'].astype('str') == '8888'] = np.nan
        df.loc[df['co_applicant_age'].astype('str') == '8888.0'] = np.nan
        df.loc[df['co_applicant_age'].astype('str') == '9999'] = np.nan
        df.loc[df['co_applicant_age'].astype('str') == '9999.0'] = np.nan

        # applicant_age_above_62
        df.loc[df['applicant_age_above_62'] == 'Yes', 'applicant_age_above_62'] = 1
        df.loc[df['applicant_age_above_62'] == 'No', 'applicant_age_above_62'] = 0
        df[f"applicant_age_above_62"] = pd.to_numeric(df[f"applicant_age_above_62"], errors="coerce").astype('Int64')

        # co_applicant_age_above_62
        df.loc[df['co_applicant_age_above_62'] == 'Yes', 'co_applicant_age_above_62'] = 1
        df.loc[df['co_applicant_age_above_62'] == 'No', 'co_applicant_age_above_62'] = 0
        df[f"coapplicant_age_above_62"] = pd.to_numeric(df[f"co_applicant_age_above_62"], errors="coerce").astype('Int64')

        # submission_of_application
        df[f"submission_of_application"] = pd.to_numeric(df[f"submission_of_application"], errors="coerce").astype('Int64')
        df.loc[df['submission_of_application'] == 1, 'submission_of_application_category'] = 'Submitted directly to institution'
        df.loc[df['submission_of_application'] == 2, 'submission_of_application_category'] = 'Not Submitted to institution'
        df.loc[df['submission_of_application'] == 3, 'submission_of_application_category'] = np.nan
        df.loc[df['submission_of_application'] == 1111, 'submission_of_application_category'] = 'Exempt'

        df.loc[df['submission_of_application'] == 1, 'submission_of_application'] = 1
        df.loc[df['submission_of_application'] == 2, 'submission_of_application'] = 0
        df.loc[df['submission_of_application'] == 3, 'submission_of_application'] = np.nan
        df.loc[df['submission_of_application'] == 11111, 'submission_of_application'] = np.nan

        # initially_payable_to_institution
        df[f"initially_payable_to_institution"] = pd.to_numeric(df[f"initially_payable_to_institution"], errors="coerce").astype('Int64')
        df.loc[df['initially_payable_to_institution'] == 1, 'initially_payable_to_institution_category'] = 'Initially Payable to Institution'
        df.loc[df['initially_payable_to_institution'] == 2, 'initially_payable_to_institution_category'] = 'Not Initially Payable to Institution'
        df.loc[df['initially_payable_to_institution'] == 3, 'initially_payable_to_institution_category'] = np.nan
        df.loc[df['initially_payable_to_institution'] == 1111, 'initially_payable_to_institution_category'] = 'Exempt'

        df.loc[df['initially_payable_to_institution'] == 1, 'initially_payable_to_institution'] = 1
        df.loc[df['initially_payable_to_institution'] == 2, 'initially_payable_to_institution'] = 0
        df.loc[df['initially_payable_to_institution'] == 3, 'initially_payable_to_institution'] = np.nan
        df.loc[df['initially_payable_to_institution'] == 1111, 'initially_payable_to_institution'] = np.nan

        # aus_X
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
            
            df.loc[df[f"aus_{x}"] == 6, f"aus_{x}"] = np.nan
            df.loc[df[f"aus_{x}"] == 1111, f"aus_{x}"] = np.nan
            
        # denial_reason_X
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
            
            df.loc[df[f"denial_reason_{x}"] == 10, f"denial_reason_{x}"] = np.nan

        # tract_population
        df['tract_population'] = pd.to_numeric(df['tract_population'], errors='coerce').astype('Int64')

        # ffiec_msa_md_median_family_income
        df['ffiec_msa_md_median_family_income'] = pd.to_numeric(df['ffiec_msa_md_median_family_income'], errors='coerce').astype('Int64')

        # tract_owner_occupied_units
        df['tract_owner_occupied_units'] = pd.to_numeric(df['tract_owner_occupied_units'], errors='coerce').astype('Int64')

        # tract_one_to_four_family_homes
        df['tract_one_to_four_family_homes'] = pd.to_numeric(df['tract_one_to_four_family_homes'], errors='coerce').astype('Int64')

        # tract_median_age_of_housing_units
        df['tract_median_age_of_housing_units'] = pd.to_numeric(df['tract_median_age_of_housing_units'], errors='coerce').astype('Int64')

        print(f"Cleaning complete - getting files ready for S3", flush=True)

        count = 0
        print(f"Processing {len(df)} rows in the dataframe", flush=True)
        for i, start_row in enumerate(range(0, len(df), chunksize)): 
            print(i)
            chunk = df.iloc[start_row:start_row + chunksize]
            table = pa.Table.from_pandas(chunk)

            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer)

            clean_key = f"{clean_prefix}{root_file_part}_{clean_file_base}_{count}.parquet"
            s3.put_object(
                Bucket=target_bucket, 
                Key=clean_key, 
                Body=parquet_buffer.getvalue()
            )

            print(f"✅ Uploaded clean Parquet chunk to s3://{target_bucket}/{clean_key}", flush=True)
            count += 1



if __name__ == "__main__": 
    parser = argparse.ArgumentParser(description='Perform data type transformations and create categories')
    parser.add_argument('--source-bucket', required=True, help='Source S3 bucket name')
    parser.add_argument('--target-bucket', required=True, help="Target S3 bucket name")
    parser.add_argument('--raw-prefix', required=True, help='Prefix of the untransformed files in S3')
    parser.add_argument('--clean-prefix', required=True, help="Prefix of the transformed files directory")
    parser.add_argument('--clean-file-base', required=True, help='The root of the file name')
    parser.add_argument('--profile', default=None, help='AWS CLI profile name')
    parser.add_argument('--chunksize', default=250_000, help="The number of rows per parquet file")

    args = parser.parse_args()

    main(
        args.source_bucket, 
        args.target_bucket, 
        args.raw_prefix, 
        args.clean_prefix, 
        args.clean_file_base, 
        profile=args.profile, 
        chunksize=args.chunksize
    )
