import argparse
import boto3
import pandas as pd
import io
import s3fs

def clean_chunk(df):
    for col in df.select_dtypes(include='object').columns:
        if df[col].dropna().apply(lambda x: isinstance(x, bytes)).any():
            df[col] = df[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if df[col].isna().all():
                df[col] = df[col].astype(str)
        except Exception:
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
                chunk = clean_chunk(chunk)

                buffer = io.BytesIO()
                chunk.to_parquet(buffer, index=False, engine='pyarrow')
                buffer.seek(0)

                base_name = raw_file.split('/')[-1].replace('.csv', '')
                clean_key = f"{clean_prefix}{base_name}_part{i}.parquet"
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
