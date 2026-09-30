"""AWS Glue Python Shell job: Silver incident Parquet -> Gold daily KPIs."""
import io
import os
import sys

import boto3
import pandas as pd

from src.transforms import incident_gold_metrics


def value(name, default):
    try:
        return sys.argv[sys.argv.index(name) + 1]
    except ValueError:
        return os.getenv(name.removeprefix("--"), default)


LAKE_BUCKET = value("--LAKE_BUCKET", "prasanna-incident-datalake-curated-361796581269")


def main():
    session = boto3.Session() if os.getenv("RUNNING_IN_GLUE") else boto3.Session(profile_name=os.getenv("AWS_PROFILE", "incident-datalake"))
    s3 = session.client("s3")
    keys = [o["Key"] for p in s3.get_paginator("list_objects_v2").paginate(Bucket=LAKE_BUCKET, Prefix="curated/incidents/")
            for o in p.get("Contents", []) if o["Key"].endswith(".parquet")]
    if not keys:
        raise RuntimeError("No Silver incident parquet files found")
    silver = pd.concat([pd.read_parquet(io.BytesIO(s3.get_object(Bucket=LAKE_BUCKET, Key=k)["Body"].read())) for k in keys])
    gold = incident_gold_metrics(silver)
    buffer = io.BytesIO()
    gold.to_parquet(buffer, index=False)
    s3.put_object(Bucket=LAKE_BUCKET, Key="gold/incident_daily_metrics/incident_daily_metrics.parquet", Body=buffer.getvalue())
    print(f"Wrote {len(gold)} Gold KPI rows")


if __name__ == "__main__":
    main()
