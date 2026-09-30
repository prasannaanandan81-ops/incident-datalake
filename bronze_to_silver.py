"""AWS Glue Python Shell job: Bronze ITSM CSVs -> Silver Parquet.

    Bronze: s3://<RAW_BUCKET>/raw/{incidents,problems,changes}/*.csv
    Silver: s3://<LAKE_BUCKET>/curated/{incidents,problems,changes}/incidents.parquet

Uses src.transforms.to_silver for cleaning, de-duplication, and lineage
columns, so the same transform is unit-tested (tests/test_transforms.py) and
shared with anything else that needs Silver data.
"""
import io
import os
import sys

import boto3
import pandas as pd

from src.transforms import to_silver

# source name -> (bronze prefix, silver prefix, timestamp column)
SOURCES = {
    "incidents": ("raw/incidents/", "curated/incidents/", "opened_at"),
    "problems": ("raw/problems/", "curated/problems/", "opened_at"),
    "changes": ("raw/changes/", "curated/changes/", "requested_at"),
}


def value(name, default):
    try:
        return sys.argv[sys.argv.index(name) + 1]
    except ValueError:
        return os.getenv(name.removeprefix("--"), default)


RAW_BUCKET = value("--RAW_BUCKET", "prasanna-incident-datalake-raw-361796581269")
LAKE_BUCKET = value("--LAKE_BUCKET", "prasanna-incident-datalake-curated-361796581269")


def get_session() -> boto3.Session:
    if os.getenv("RUNNING_IN_GLUE"):
        return boto3.Session()
    return boto3.Session(profile_name=os.getenv("AWS_PROFILE", "incident-datalake"))


def read_source_csvs(s3, prefix: str) -> pd.DataFrame:
    keys = [
        o["Key"]
        for p in s3.get_paginator("list_objects_v2").paginate(Bucket=RAW_BUCKET, Prefix=prefix)
        for o in p.get("Contents", [])
        if o["Key"].endswith(".csv")
    ]
    if not keys:
        return pd.DataFrame()
    frames = [pd.read_csv(io.BytesIO(s3.get_object(Bucket=RAW_BUCKET, Key=k)["Body"].read())) for k in keys]
    return pd.concat(frames, ignore_index=True)


def main():
    session = get_session()
    s3 = session.client("s3")

    for name, (bronze_prefix, silver_prefix, timestamp_col) in SOURCES.items():
        raw = read_source_csvs(s3, bronze_prefix)
        if raw.empty:
            print(f"{name}: no CSV files found under s3://{RAW_BUCKET}/{bronze_prefix}")
            continue

        silver = to_silver(raw, name, timestamp_col)
        buffer = io.BytesIO()
        silver.to_parquet(buffer, index=False)
        key = f"{silver_prefix}{name}.parquet"
        s3.put_object(Bucket=LAKE_BUCKET, Key=key, Body=buffer.getvalue())
        print(f"{name}: wrote {len(silver)} Silver rows -> s3://{LAKE_BUCKET}/{key}")


if __name__ == "__main__":
    main()
