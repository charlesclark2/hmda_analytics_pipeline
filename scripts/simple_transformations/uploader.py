# uploader.py
import io
import pyarrow as pa
import pyarrow.parquet as pq


def upload_chunks_to_s3(df, s3_client, bucket, prefix, base_filename, chunksize):
    count = 0
    for start in range(0, len(df), chunksize):
        chunk = df.iloc[start:start + chunksize]
        table = pa.Table.from_pandas(chunk)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)

        key = f"{prefix}{base_filename}_{count}.parquet"
        s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        print(f"✅ Uploaded chunk to s3://{bucket}/{key}", flush=True)

        count += 1
