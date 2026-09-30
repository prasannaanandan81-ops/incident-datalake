"""Pure medallion transformations, shared by local runs and AWS Glue."""

import pandas as pd


def to_silver(frame: pd.DataFrame, source: str, timestamp_column: str) -> pd.DataFrame:
    """Clean, type, and deduplicate one ITSM source into a Silver data set."""
    df = frame.copy()
    df.columns = [column.strip().lower() for column in df.columns]
    df = df.drop_duplicates().dropna(how="all")
    df[timestamp_column] = pd.to_datetime(df[timestamp_column], errors="coerce")
    df = df.dropna(subset=[timestamp_column])
    df["source_system"] = "synthetic-servicenow"
    df["record_type"] = source
    df["ingested_at"] = pd.Timestamp.now(tz="UTC")
    df["event_date"] = df[timestamp_column].dt.date.astype(str)
    return df.sort_values(timestamp_column).reset_index(drop=True)


def incident_gold_metrics(incidents: pd.DataFrame) -> pd.DataFrame:
    """Create dashboard-ready daily incident KPIs from the Silver incidents table."""
    df = incidents.copy()
    df["opened_at"] = pd.to_datetime(df["opened_at"], errors="coerce")
    df["resolved_at"] = pd.to_datetime(df["resolved_at"], errors="coerce")
    df["resolution_hours"] = (df["resolved_at"] - df["opened_at"]).dt.total_seconds() / 3600
    df["is_open"] = ~df["status"].isin(["Resolved", "Closed"])
    metrics = (
        df.groupby([df["opened_at"].dt.date.rename("opened_date"), "priority", "category", "assignment_group"], dropna=False)
        .agg(
            incident_count=("incident_number", "nunique"),
            open_incident_count=("is_open", "sum"),
            avg_resolution_hours=("resolution_hours", "mean"),
        )
        .reset_index()
    )
    metrics["opened_date"] = metrics["opened_date"].astype(str)
    metrics["avg_resolution_hours"] = metrics["avg_resolution_hours"].round(2)
    return metrics
