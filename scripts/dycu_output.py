import argparse 
import pandas as pd
import numpy as np
import s3fs
import boto3
import io

def read_parquet_to_pandas(bucket, prefix, target_df, profile=None): 
    fs = s3fs.S3FileSystem(profile=profile)
    parquet_files = fs.ls(f"{bucket}/{prefix}")
    
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client('s3')

    # Get the target census tracts from the DYCU output
    target_df['census_tract'] = target_df['census_tract'].astype('string')
    target_tracts = target_df.census_tract.unique().tolist()
    target_tracts = list(map(str, target_tracts))
    
    all_chunks = []

    for pq_file in parquet_files: 
        print(f"🔍 Looking for matching census tracts in: s3://{pq_file}")

        with fs.open(pq_file, 'rb') as f: 
            df = pd.read_parquet(f, engine='pyarrow')
            df['census_tract'] = (
                pd.to_numeric(df['census_tract'], errors='coerce')  # convert '37057061600.0' → 37057061600.0
                .astype('Int64')                                    # nullable integer type (allows NaNs)
                .astype(str)                                        # final string version for matching
            )
            df = df[df['census_tract'].notna() & df['census_tract'].isin(target_tracts)]
            
            all_chunks.append(df)
    
    return pd.concat(all_chunks, ignore_index=True)


def get_target_df(refined_bucket, target_path, profile=None): 
    fs = s3fs.S3FileSystem(profile=profile)
    with fs.open(f"{refined_bucket}/{target_path}", 'rb') as f: 
        target_df = pd.read_excel(f)
    return target_df


def normalize_dataframe_for_parquet(df):
    for col in df.select_dtypes(include='object').columns:
        # Decode bytes if needed
        if df[col].dropna().apply(lambda x: isinstance(x, bytes)).any():
            df[col] = df[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

        # Try converting to numeric if possible
        sample = df[col].dropna().head(50)
        numeric_ratio = pd.to_numeric(sample, errors='coerce').notna().mean()
        if numeric_ratio > 0.9:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = df[col].astype(str)

    return df



def send_dataframe_to_s3(df, output_type, target_bucket, target_prefix, profile=None): 
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client('s3')

    buffer = io.BytesIO()
    if output_type == 'parquet': 
        df = normalize_dataframe_for_parquet(df)
        df.to_parquet(buffer, index=False, engine='pyarrow')
        file_suffix = '.parquet'
    elif output_type == 'csv': 
        df.to_csv(buffer, index=False)
        file_suffix = '.csv'
    buffer.seek(0)

    target_key = f"{target_prefix}{file_suffix}"
    s3.put_object(Bucket=target_bucket, Key=target_key, Body=buffer.getvalue())


def create_output(df, target_bucket, mke_output_prefix, mke_summary_output_prefix, profile=None): 
    print(f"Matching tracts in dataframe: {len(df.census_tract.unique().tolist())}")
    df.drop_duplicates(inplace=True)

    # Send this processed MKE dataframe to S3 as parquet
    send_dataframe_to_s3(df, 'parquet', target_bucket, mke_output_prefix, profile)
    print(f"✅ Uploaded MKE output to s3://{target_bucket}/{mke_output_prefix}.parquet")

    df = df.query("loan_purpose == 1 & occupancy_type == 1")
    tract_summary = (
        df.groupby("census_tract")
        .apply(lambda g: pd.Series({
            "TotalOriginations": (g["action_taken"] == 1).sum(),
            "WhiteNonHispanicOriginations": ((g["action_taken"] == 1) & (g["applicant_race_1"] == 5) & (g['applicant_ethnicity_1'] == 1)).sum(),
            "BlackOriginations": ((g["action_taken"] == 1) & (g["applicant_race_1"] == 3)).sum(),
            "LatinxOriginations": ((g["action_taken"] == 1) & (g["applicant_ethnicity_1"].isin([1, 11, 12, 13, 14]))).sum(),
            "AAPIOriginations": ((g["action_taken"] == 1) & (g["applicant_race_1"].isin([2, 4, 21, 22, 23, 24, 25, 26, 27]))).sum(),
            "Applications": len(g),
            "Denials": (g["action_taken"] == 3).sum(),
            "Fallout": g["action_taken"].isin([4, 5]).sum(),
            
        }))
        .reset_index()
    )
    tract_summary['OriginationRate'] = round(tract_summary['TotalOriginations'] / tract_summary['Applications'], 6)
    tract_summary['DenialRate'] = round(tract_summary['Denials'] / tract_summary['Applications'], 6)
    tract_summary['FalloutRate'] = round(tract_summary['Fallout'] / tract_summary['Applications'], 6)
    
    # Send summary CSV to S3
    send_dataframe_to_s3(tract_summary, 'csv', target_bucket, mke_summary_output_prefix, profile)
    print(f"✅ Uploaded MKE summary output to s3://{target_bucket}/{mke_output_prefix}.csv")


if __name__ == "__main__": 
    parser = argparse.ArgumentParser(description="Clean and convert raw CSV to Parquet in S3")
    parser.add_argument('--bucket', required=True, help="Source S3 bucket name")
    parser.add_argument('--prefix', required=True, help="The prefix where the files are located")
    parser.add_argument('--refined-bucket', required=True, help="Bucket where refined data lands")
    parser.add_argument('--mke-output-prefix', required=True, help="The prefix for the MKE parquet output")
    parser.add_argument('--mke-summary-prefix', required=True, help="Summary output prefix in DYCU format in CSV")
    parser.add_argument('--target-output-path', default='dycu_outputs/HMDA_2022_CTData.xls')
    parser.add_argument('--profile', default=None, help="AWS CLI profile name")
    args = parser.parse_args()

    target_df = get_target_df(args.refined_bucket, args.target_output_path, profile=args.profile)

    df = read_parquet_to_pandas(args.bucket, args.prefix, target_df, profile=args.profile)

    create_output(df, args.refined_bucket, args.mke_output_prefix, args.mke_summary_prefix, profile=args.profile)
