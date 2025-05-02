import argparse
import boto3
import requests
import zipfile
import io
import pandas as pd

def upload_raw_csv_chunks(url, bucket, s3_prefix='raw/hmda/2023/', chunksize=250_000, profile=None):
    print(f"Getting file from {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for file_info in z.infolist():
            if file_info.is_dir() or not file_info.filename.endswith('.csv'):
                continue

            with z.open(file_info.filename) as raw_file:
                print(f"Streaming {file_info.filename}")
                text_stream = io.TextIOWrapper(raw_file, encoding='utf-8')
                reader = pd.read_csv(text_stream, chunksize=chunksize, low_memory=False)

                session = boto3.Session(profile_name=profile) if profile else boto3.Session()
                s3 = session.client('s3')

                for i, chunk in enumerate(reader):
                    buffer = io.StringIO()
                    chunk.to_csv(buffer, index=False)
                    buffer.seek(0)

                    s3_key = f"{s3_prefix}{file_info.filename.replace('.csv', '')}_part{i}.csv"
                    s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue())

                    print(f"✅ Uploaded raw CSV chunk to s3://{bucket}/{s3_key}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest raw CSV from ZIP to S3")
    parser.add_argument('--url', required=True, help="URL of ZIP file containing CSV")
    parser.add_argument('--bucket', required=True, help="S3 bucket to upload to")
    parser.add_argument('--prefix', default='raw/hmda/2023/', help="S3 prefix for raw CSV files")
    parser.add_argument('--profile', default=None, help="AWS CLI profile name")
    args = parser.parse_args()

    upload_raw_csv_chunks(args.url, args.bucket, args.prefix, profile=args.profile)

# python ingest_to_s3.py
#  --source_url https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2023/2023_public_lar_csv.zip 
# --aws_profile_name AdministratorAccess-769392325318 
# --target_s3_bucket_name mortgage-data-raw 
# --target_s3_prefix hmda/2023/public_lar 
# --source_file_type zip