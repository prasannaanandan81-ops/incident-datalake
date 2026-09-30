# Incident Data Lake: Bronze → Silver → Gold

A serverless ITSM analytics mini-project: generate ServiceNow-style records, land them in S3, transform them with Glue, query KPIs with Athena, and visualize them in QuickSight.

```text
Synthetic CSV → S3 raw (Bronze) → Glue → S3 curated (Silver Parquet) → Glue → S3 gold (KPIs) → Athena → QuickSight
                                      ↑
                         GitHub Actions CI/CD + CloudFormation
```

## Included

- `generate_data.py`: synthetic incidents, problems, and changes.
- `bronze_to_silver.py`: CSV-to-date-partitioned Parquet Glue job.
- `silver_to_gold.py`: dashboard-ready daily incident KPIs.
- `src/transforms.py`: tested cleaning, lineage, de-duplication, and KPI aggregation.
- `infrastructure/incident-lake.yaml`: Glue Catalog, jobs, Athena workgroup, and IAM role, using existing buckets.
- `.github/workflows/ci-cd.yml`: tests on PR/push and an approval-gated manual deployment.

## Validate locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
python generate_data.py
python bronze_to_silver.py
python silver_to_gold.py
```

The AWS profile currently returns `InvalidClientTokenId`; refresh its credentials before the S3/Glue commands.

## Deploy with GitHub Actions

Set repository variables: `AWS_REGION`, `AWS_DEPLOY_ROLE_ARN`, `RAW_BUCKET`, `LAKE_BUCKET`, and `ARTIFACT_BUCKET`. The deploy role needs GitHub Actions OIDC trust plus CloudFormation, Glue, IAM role creation/passing, and artifact-bucket permissions. Run **Incident lake CI/CD** manually with `deploy` enabled. Protect the `production` environment if an approval is desired.

Run the Bronze-to-Silver job, then Silver-to-Gold. In Athena, select workgroup `incident-lake` and database `incident_lake`; use [`athena/queries.sql`](athena/queries.sql). In QuickSight, create an Athena dataset from `incident_daily_metrics` and add daily incidents, open backlog by priority, and resolution time by assignment group.

GitHub Actions is used because GitHub is already integrated and OIDC avoids long-lived AWS secrets. Jenkins or Spinnaker can replace its deploy stage where an organization mandates them, but should not control the same stack concurrently.
