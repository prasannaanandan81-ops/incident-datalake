# Incident Data Lake — Serverless AWS ETL Pipeline

A small end-to-end AWS data lake project simulating an ITSM (ServiceNow-style) data pipeline:
Incidents, Problems, and Changes flow from a raw landing zone through a serverless ETL
transform into a query-ready analytics layer.

## Architecture

```
Python (boto3) generator
        │
        ▼
   S3 raw/  (CSV, partitioned by source)
        │
        ▼
  AWS Glue ETL Job  ──►  AWS Glue Crawler  ──►  Glue Data Catalog
        │
        ▼
  S3 curated/  (Parquet, partitioned by year/month/day)
        │
        ▼
     Amazon Athena  (SQL queries)
        │
        ▼
   Amazon QuickSight  (dashboard)
```

An AWS Lambda function, triggered on new object creation in `raw/`, kicks off the Glue job
automatically — no manual/batch scheduling step required (the cloud-native equivalent of an
Autosys job).

## Why this pattern

This mirrors a common enterprise data lake pattern: land raw operational data (incident,
problem, change records) in object storage, transform it into a columnar, partitioned format
(Parquet) for efficient querying, catalog it so analytics engines can discover its schema, and
serve it through both ad hoc SQL (Athena) and a BI dashboard (QuickSight) — without provisioning
or managing any servers.

## Stack

| Layer | Service | Purpose |
|---|---|---|
| Ingestion | Python + boto3 | Generates synthetic ITSM data, uploads to S3 |
| Storage (raw) | Amazon S3 | Landing zone, encrypted (SSE-S3), versioned, private |
| Orchestration | AWS Lambda | Event-driven trigger on new file arrival |
| Transform | AWS Glue (ETL Job + Crawler) | CSV → partitioned Parquet, schema cataloging |
| Storage (curated) | Amazon S3 | Query-optimized Parquet, partitioned by date |
| Query | Amazon Athena | Serverless SQL over the Glue Data Catalog |
| Visualization | Amazon QuickSight | Dashboard for business users |

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

aws configure --profile incident-datalake   # region: us-east-2

python generate_data.py
```

## Project status

- [x] AWS account, IAM user, CLI configured
- [x] S3 raw/curated buckets (encrypted, versioned, public access blocked)
- [x] Synthetic incident/problem/change data generator (Python + boto3)
- [ ] Glue ETL job (CSV → partitioned Parquet)
- [ ] Glue Crawler + Data Catalog
- [ ] Athena queries
- [ ] Lambda event trigger
- [ ] QuickSight dashboard

## Author

Prasanna Anandan — 20+ years in L2 production support, incident/problem/change management,
and AWS cloud data platforms. Built as a hands-on project to demonstrate serverless data
engineering patterns end to end.
