import argparse
import boto3
import pandas as pd
import s3fs
import concurrent.futures
from collections import OrderedDict
from cleaner import clean_dataframe, enforce_column_types, validate_schema
from uploader import upload_chunks_to_s3

def clean_and_validate_file(file_path, fs):
    try:
        print(f"🧹 Cleaning {file_path}", flush=True)
        df = pd.read_parquet(f"s3://{file_path}", engine="pyarrow", filesystem=fs)
        df.drop_duplicates(inplace=True)
        df = df.reset_index(drop=True)
        df = clean_dataframe(df)
        return file_path, df, None
    except Exception as e:
        return file_path, None, e

def normalize_schema(dtypes):
    return OrderedDict(sorted({k: str(v) for k, v in dtypes.items()}.items()))

def coerce_schema(df, reference_schema):
    for col, expected_type in reference_schema.items():
        if col not in df.columns:
            df[col] = pd.Series([pd.NA] * len(df), dtype=expected_type)
        else:
            try:
                df[col] = df[col].astype(expected_type)
            except Exception:
                try:
                    # Convert 'inf' and other non-finite to NaN first
                    series = pd.to_numeric(df[col], errors='coerce')
                    series = series.replace([float('inf'), float('-inf')], pd.NA)
                    if expected_type.startswith("Int"):
                        df[col] = series.astype("Int64")
                    else:
                        df[col] = series.astype(expected_type)
                except Exception as fallback_err:
                    print(f"⚠️ Failed to cast column {col} to {expected_type}: {fallback_err}")
                    df[col] = pd.Series([pd.NA] * len(df), dtype="object")
    return df


def process_parquet_files(source_bucket, target_bucket, raw_prefix, clean_prefix, clean_file_base, profile=None, chunksize=250_000, max_workers=4):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client('s3')
    fs = s3fs.S3FileSystem(profile=profile)

    print(f"📂 Fetching files from s3://{source_bucket}/{raw_prefix}", flush=True)
    parquet_files = fs.glob(f"{source_bucket}/{raw_prefix}*.parquet")

    cleaned_data = []
    schema_reference = None
    schema_report = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(clean_and_validate_file, file_path, fs) for file_path in parquet_files]
        for future in concurrent.futures.as_completed(futures):
            file_path, df, err = future.result()
            if err:
                print(f"❌ Failed to process {file_path}: {err}", flush=True)
                schema_report.append((file_path, "LoadError", str(err)))
                continue

            current_schema = normalize_schema(df.dtypes)

            if schema_reference is None:
                schema_reference = current_schema
            elif current_schema != schema_reference:
                print(f"⚠️ Schema mismatch in {file_path}, coercing to match reference...", flush=True)
                mismatches = []
                for col in current_schema:
                    if col not in schema_reference:
                        mismatches.append((col, "Unexpected", current_schema[col], None))
                    elif current_schema[col] != schema_reference[col]:
                        mismatches.append((col, "TypeMismatch", current_schema[col], schema_reference[col]))
                for col in schema_reference:
                    if col not in current_schema:
                        mismatches.append((col, "Missing", None, schema_reference[col]))
                schema_report.append((file_path, "SchemaMismatch", mismatches))
                df = coerce_schema(df, schema_reference)

            cleaned_data.append((file_path, df))

    print("✅ Schema validation complete. Proceeding with upload...", flush=True)
    for file_path, df in cleaned_data:
        root_part = file_path.split('/')[-1].split('_')[-1].replace('.parquet', '')
        print(f"📤 Uploading cleaned file in chunks for {file_path}", flush=True)
        upload_chunks_to_s3(df, s3, target_bucket, clean_prefix, f"{clean_file_base}_{root_part}", chunksize)

    print("\n📊 Schema Audit Report:")
    for file_path, issue_type, details in schema_report:
        print(f"- {file_path}: {issue_type}")
        if issue_type == "SchemaMismatch":
            for col, kind, actual, expected in details:
                if kind == "Unexpected":
                    print(f"    ➕ Unexpected column: {col} ({actual})")
                elif kind == "Missing":
                    print(f"    ➖ Missing column: {col} (expected {expected})")
                elif kind == "TypeMismatch":
                    print(f"    🔁 Type mismatch: {col} ({actual} vs {expected})")
        elif issue_type == "LoadError":
            print(f"    ⚠️ {details}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean and upload HMDA parquet files in chunks")
    parser.add_argument("--source-bucket", required=True)
    parser.add_argument("--target-bucket", required=True)
    parser.add_argument("--raw-prefix", required=True)
    parser.add_argument("--clean-prefix", required=True)
    parser.add_argument("--clean-file-base", required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--chunksize", type=int, default=250_000)
    parser.add_argument("--max-workers", type=int, default=4)

    args = parser.parse_args()

    process_parquet_files(
        args.source_bucket,
        args.target_bucket,
        args.raw_prefix,
        args.clean_prefix,
        args.clean_file_base,
        profile=args.profile,
        chunksize=args.chunksize,
        max_workers=args.max_workers
    )