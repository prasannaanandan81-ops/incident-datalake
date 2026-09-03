"""
generate_data.py

Generates synthetic ITSM-style data (Incidents, Problems, Changes) that mimics
a ServiceNow export, and uploads it as CSV to the S3 "raw" landing bucket.

This simulates the ingestion step of the data lake pattern:
    ITSM system export -> S3 raw/ -> (later) Glue ETL -> S3 curated/ (Parquet) -> Athena -> QuickSight

Usage:
    source venv/bin/activate
    python generate_data.py
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import boto3
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

AWS_PROFILE = "incident-datalake"
RAW_BUCKET = "prasanna-incident-datalake-raw-361796581269"
OUTPUT_DIR = Path("data_out")

PRIORITIES = ["P1 - Critical", "P2 - High", "P3 - Moderate", "P4 - Low"]
CATEGORIES = ["Network", "Database", "Application", "Server", "Storage", "Security"]
ASSIGNMENT_GROUPS = [
    "L2-Prod-Support", "DBA-Team", "Network-Ops", "App-Support",
    "Cloud-Platform-Team", "Security-Ops",
]
STATUSES_INCIDENT = ["New", "In Progress", "On Hold", "Resolved", "Closed"]
STATUSES_PROBLEM = ["Open", "Root Cause Identified", "Fix In Progress", "Closed"]
STATUSES_CHANGE = ["Requested", "Approved", "Scheduled", "Implemented", "Closed", "Rejected"]
CHANGE_TYPES = ["Standard", "Normal", "Emergency"]

TODAY = datetime(2026, 9, 3)


def random_datetime_within(days_back: int) -> datetime:
    start = TODAY - timedelta(days=days_back)
    delta_seconds = int((TODAY - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta_seconds))


def gen_incidents(n: int):
    rows = []
    for i in range(n):
        opened = random_datetime_within(90)
        priority = random.choice(PRIORITIES)
        status = random.choice(STATUSES_INCIDENT)
        resolved = None
        if status in ("Resolved", "Closed"):
            resolved = opened + timedelta(hours=random.randint(1, 72))
        rows.append({
            "incident_number": f"INC{100000 + i}",
            "short_description": fake.sentence(nb_words=8),
            "priority": priority,
            "category": random.choice(CATEGORIES),
            "assignment_group": random.choice(ASSIGNMENT_GROUPS),
            "status": status,
            "opened_at": opened.strftime("%Y-%m-%d %H:%M:%S"),
            "resolved_at": resolved.strftime("%Y-%m-%d %H:%M:%S") if resolved else "",
            "opened_by": fake.user_name(),
            "assigned_to": fake.user_name(),
            "year": opened.year,
            "month": f"{opened.month:02d}",
            "day": f"{opened.day:02d}",
        })
    return rows


def gen_problems(n: int):
    rows = []
    for i in range(n):
        opened = random_datetime_within(120)
        status = random.choice(STATUSES_PROBLEM)
        rows.append({
            "problem_number": f"PRB{200000 + i}",
            "short_description": fake.sentence(nb_words=8),
            "related_incident": f"INC{100000 + random.randint(0, 499)}",
            "category": random.choice(CATEGORIES),
            "assignment_group": random.choice(ASSIGNMENT_GROUPS),
            "status": status,
            "root_cause": fake.sentence(nb_words=10) if status != "Open" else "",
            "opened_at": opened.strftime("%Y-%m-%d %H:%M:%S"),
            "year": opened.year,
            "month": f"{opened.month:02d}",
            "day": f"{opened.day:02d}",
        })
    return rows


def gen_changes(n: int):
    rows = []
    for i in range(n):
        opened = random_datetime_within(120)
        status = random.choice(STATUSES_CHANGE)
        implemented = None
        if status in ("Implemented", "Closed"):
            implemented = opened + timedelta(days=random.randint(1, 14))
        rows.append({
            "change_number": f"CHG{300000 + i}",
            "short_description": fake.sentence(nb_words=8),
            "change_type": random.choice(CHANGE_TYPES),
            "category": random.choice(CATEGORIES),
            "assignment_group": random.choice(ASSIGNMENT_GROUPS),
            "status": status,
            "risk": random.choice(["Low", "Moderate", "High"]),
            "requested_at": opened.strftime("%Y-%m-%d %H:%M:%S"),
            "implemented_at": implemented.strftime("%Y-%m-%d %H:%M:%S") if implemented else "",
            "requested_by": fake.user_name(),
            "year": opened.year,
            "month": f"{opened.month:02d}",
            "day": f"{opened.day:02d}",
        })
    return rows


def write_csv(rows: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  wrote {len(rows):>5} rows -> {path}")


def upload_to_s3(local_path: Path, s3_key: str):
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3 = session.client("s3")
    s3.upload_file(str(local_path), RAW_BUCKET, s3_key)
    print(f"  uploaded -> s3://{RAW_BUCKET}/{s3_key}")


def main():
    print("Generating synthetic ITSM data...")

    incidents = gen_incidents(500)
    problems = gen_problems(150)
    changes = gen_changes(200)

    run_id = uuid.uuid4().hex[:8]
    files = {
        "incidents": (incidents, OUTPUT_DIR / f"incidents_{run_id}.csv", f"raw/incidents/incidents_{run_id}.csv"),
        "problems": (problems, OUTPUT_DIR / f"problems_{run_id}.csv", f"raw/problems/problems_{run_id}.csv"),
        "changes": (changes, OUTPUT_DIR / f"changes_{run_id}.csv", f"raw/changes/changes_{run_id}.csv"),
    }

    for name, (rows, local_path, s3_key) in files.items():
        print(f"\n{name}:")
        write_csv(rows, local_path)
        upload_to_s3(local_path, s3_key)

    print("\nDone. Data landed in s3://%s/raw/{incidents,problems,changes}/" % RAW_BUCKET)


if __name__ == "__main__":
    main()
