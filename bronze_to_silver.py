"""
glue_etl_job.py

ETL transform step of the data lake pipeline: reads raw ITSM CSVs from
S3 raw/, converts them to partitioned Parquet, and writes them to S3 curated/.

    S3 raw/ (CSV)  ->  this script  ->  S3 curated/ (Parquet, partitioned by year/month/day)

Written as a Glue Python Shell job (pandas + pyarrow), not PySpark — the data
volume here doesn't need a Spark cluster, and pandas/pyarrow are already the
project's dependencies (see requirements.txt).

Local run (against real S3, using the incident-datalake profile):
    source venv/bin/activate
    python glue_etl_job.py

As an actual AWS Glue Python Shell job, the AWS_PROFILE env var is unset/unused —
Glue's job role supplies credentials automatically; boto3.Session() picks those
up with no profile_name needed. See `get_session()` below.
"""

import io
import os

import boto3
import pandas as pd

RAW_BUCKET = "prasanna-incident-datalake-raw-361796581269"
CURATED_BUCKET = "prasanna-incident-datalake-curated-361796581269"
AWS_PROFILE = "incident-datalake"

# source name -> (raw prefix, curated prefix, date column used for partitioning)
SOURCES = {
    "incidents": ("raw/incidents/", "curated/incidents/", "opened_at"),
    "problems": ("raw/problems/", "curated/problems/", "opened_at"),
    "changes": ("raw/changes/", "curated/changes/", "requested_at"),
}


def get_session() -> boto3.Session:
    # Running locally: use the named CLI profile. Running inside AWS Glue: no
    # profile exists there, so fall back to the job's own IAM role credentials.
    if os.environ.get("RUNNING_IN_GLUE"):
        return boto3.Session()
    return boto3.Session(profile_name=AWS_PROFILE)


def list_csv_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                keys.append(obj["Key"])
    return keys


def read_csv_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def write_partitioned_parquet(s3, df: pd.DataFrame, curated_prefix: str, date_col: str):
    """Partition df by year/month/day (already present as columns from the
    generator) and write one Parquet file per partition to S3."""
    df = df.copy()
    # year/month/day columns come pre-computed (zero-padded strings) from
    # generate_data.py, but pd.read_csv re-infers "06"/"05" back to int64 and
    # drops the padding. Always (re)derive from date_col so partition keys
    # stay zero-padded and Hive/Athena-sortable regardless of dtype on read.
    parsed = pd.to_datetime(df[date_col])
    df["year"] = parsed.dt.year
    df["month"] = parsed.dt.strftime("%m")
    df["day"] = parsed.dt.strftime("%d")

    written = 0
    for (year, month, day), group in df.groupby(["year", "month", "day"]):
        key = (
            f"{curated_prefix}year={year}/month={month}/day={day}/"
            f"part-{written:04d}.parquet"
        )
        buf = io.BytesIO()
        group.to_parquet(buf, engine="pyarrow", index=False)
        buf.seek(0)
        s3.put_object(Bucket=CURATED_BUCKET, Key=key, Body=buf.getvalue())
        print(f"    wrote {len(group):>5} rows -> s3://{CURATED_BUCKET}/{key}")
        written += 1
    return written


def process_source(s3, name: str, raw_prefix: str, curated_prefix: str, date_col: str):
    print(f"\n{name}:")
    keys = list_csv_keys(s3, RAW_BUCKET, raw_prefix)
    if not keys:
        print(f"  no CSV files found under s3://{RAW_BUCKET}/{raw_prefix}")
        return

    frames = [read_csv_from_s3(s3, RAW_BUCKET, k) for k in keys]
    df = pd.concat(frames, ignore_index=True)
    print(f"  read {len(df)} rows from {len(keys)} file(s)")

    partitions = write_partitioned_parquet(s3, df, curated_prefix, date_col)
    print(f"  wrote {partitions} partition file(s)")


def main():
    print("Running Glue ETL: raw CSV -> curated Parquet")
    session = get_session()
    s3 = session.client("s3")

    for name, (raw_prefix, curated_prefix, date_col) in SOURCES.items():
        process_source(s3, name, raw_prefix, curated_prefix, date_col)

    print(f"\nDone. Curated data in s3://{CURATED_BUCKET}/curated/{{incidents,problems,changes}}/")


if __name__ == "__main__":
    main()
