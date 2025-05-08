import argparse
import boto3
import pandas as pd
import io
import s3fs

def clean_chunk(df, numeric_threshold=0.9, preserve_str_columns=None):
    """
    Converts object columns to numeric only if >90% of sampled values are numeric-like.
    Explicitly preserves certain string ID columns.
    """
    if preserve_str_columns is None:
        preserve_str_columns = ['lei', 'respondent_id', 'loan_id']  # add more if needed

    for col in df.select_dtypes(include='object').columns:
        if col in preserve_str_columns:
            df[col] = df[col].astype(str)
            continue

        # Decode bytes if needed
        if df[col].dropna().apply(lambda x: isinstance(x, bytes)).any():
            df[col] = df[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

        # Sample-based numeric test
        sample = df[col].dropna().head(100)
        try_numeric = pd.to_numeric(sample, errors='coerce')
        numeric_ratio = try_numeric.notna().mean()

        if numeric_ratio >= numeric_threshold:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = df[col].astype(str)

    return df


def clean_and_convert_to_parquet(source_bucket, target_bucket, raw_prefix, clean_prefix, profile=None):
    fs = s3fs.S3FileSystem(profile=profile)
    raw_files = fs.ls(f"{source_bucket}/{raw_prefix}")

    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client('s3')

    for raw_file in raw_files:
        print(f"🔍 Processing: s3://{raw_file}")
        with fs.open(raw_file, 'r') as f:
            reader = pd.read_csv(f, chunksize=250_000, low_memory=False)

            for i, chunk in enumerate(reader):
                chunk = clean_chunk(
                    chunk, 
                    preserve_str_columns=['lei', 'derived_msa_md', 'state_code', 'county_code', 
                                          'census_tract', 'conforming_loan_limit', 'derived_loan_product_type', 
                                          'derived_dwelling_category', 'derived_ethnicity', 
                                          'derived_race', 'derived_sex', 'loan_amount', 
                                          'combined_loan_to_value_ratio', 'interest_rate', 'rate_spread', 
                                          'total_loan_costs', 'total_points_and_fees', 'origination_charges', 
                                          'discount_points', 'lender_credits', 'loan_term', 
                                          'prepayment_penalty_term', 'intro_rate_period', 
                                          'property_value', 'total_units', 'multifamily_affordable_units', 
                                          'income', 'debt_to_income_ratio', 'applicant_ethnicity_1', 
                                          'applicant_ethnicity_2', 'applicant_ethnicity_3', 
                                          'applicant_ethnicity_4', 'applicant_ethnicity_5', 
                                          'co_applicant_ethnicity_1', 'co_applicant_ethnicity_2', 
                                          'co_applicant_ethnicity_3', 'co_applicant_ethnicity_4', 
                                          'co_applicant_ethnicity_5', 'applicant_ethnicity_observed', 
                                          'co_applicant_ethnicity_observed', 'applicant_race_1', 
                                          'applicant_race_2', 'applicant_race_3', 'applicant_race_4', 
                                          'applicant_race_5', 'co_applicant_race_1', 'co_applicant_race_2', 
                                          'co_applicant_race_3', 'co_applicant_race_4', 'co_applicant_race_5', 
                                          'applicant_age', 'co_applicant_age', 'applicant_age_above_62', 
                                          'co_applicant_age_above_62', 'submission_of_application', 
                                          'initially_payable_to_institution', 'aus_2', 'aus_3', 
                                          'aus_4', 'aus_5', 'denial_reason_1', 'denial_reason_2', 
                                          'denial_reason_3', 'denial_reason_4', 'denial_reason_5']
                )

                buffer = io.BytesIO()
                chunk.to_parquet(buffer, index=False, engine='pyarrow')
                buffer.seek(0)

                base_name = raw_file.split('/')[-1].replace('.csv', '')
                clean_key = f"{clean_prefix}/{base_name}.parquet"
                s3.put_object(Bucket=target_bucket, Key=clean_key, Body=buffer.getvalue())

                print(f"✅ Uploaded clean Parquet chunk to s3://{target_bucket}/{clean_key}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean and convert raw CSV to Parquet in S3")
    parser.add_argument('--source-bucket', required=True, help="Source S3 bucket name")
    parser.add_argument('--target-bucket', required=True, help="Target S3 bucket name")
    parser.add_argument('--raw-prefix', default='raw/hmda/2023/', help="Prefix of raw CSV files in S3")
    parser.add_argument('--clean-prefix', default='clean/hmda/2023/', help="Prefix for clean Parquet output in S3")
    parser.add_argument('--profile', default=None, help="AWS CLI profile name")
    args = parser.parse_args()

    clean_and_convert_to_parquet(args.source_bucket, args.target_bucket, args.raw_prefix, args.clean_prefix, profile=args.profile)
