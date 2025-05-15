# main.py
import argparse
import boto3
import pandas as pd
import s3fs
from cleaner import clean_dataframe
from uploader import upload_chunks_to_s3

def process_parquet_files(source_bucket, target_bucket, raw_prefix, clean_prefix, clean_file_base, profile=None, chunksize=250_000):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client('s3')
    fs = s3fs.S3FileSystem(profile=profile)

    print(f"Getting files from s3://{source_bucket}/{raw_prefix}", flush=True)
    parquet_files = fs.glob(f"{source_bucket}/{raw_prefix}*.parquet")

    for file_path in parquet_files:
        root_part = file_path.split('/')[-1].split('_')[-1].replace('.parquet', '')
        print(f"Processing {file_path}", flush=True)

        df = pd.read_parquet(f"s3://{file_path}", engine="pyarrow", filesystem=fs)
        df.drop_duplicates(inplace=True)
        df = df.reset_index(drop=True)

        df = clean_dataframe(df)

        print(f"Uploading cleaned file in chunks", flush=True)
        upload_chunks_to_s3(df, s3, target_bucket, clean_prefix, f"{clean_file_base}_{root_part}", chunksize)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean and upload HMDA parquet files in chunks")
    parser.add_argument("--source-bucket", required=True)
    parser.add_argument("--target-bucket", required=True)
    parser.add_argument("--raw-prefix", required=True)
    parser.add_argument("--clean-prefix", required=True)
    parser.add_argument("--clean-file-base", required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--chunksize", type=int, default=250_000)

    args = parser.parse_args()

    process_parquet_files(
        args.source_bucket,
        args.target_bucket,
        args.raw_prefix,
        args.clean_prefix,
        args.clean_file_base,
        profile=args.profile,
        chunksize=args.chunksize
    )
