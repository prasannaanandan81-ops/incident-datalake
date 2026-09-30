import pandas as pd

from src.transforms import incident_gold_metrics, to_silver


def test_silver_removes_duplicate_and_adds_lineage():
    raw = pd.DataFrame({"incident_number": ["INC1", "INC1"], "opened_at": ["2026-09-01 01:00:00"] * 2})
    result = to_silver(raw, "incidents", "opened_at")
    assert len(result) == 1
    assert result.loc[0, "record_type"] == "incidents"
    assert result.loc[0, "event_date"] == "2026-09-01"


def test_gold_calculates_open_and_resolution_metrics():
    silver = pd.DataFrame({
        "incident_number": ["INC1", "INC2"],
        "opened_at": ["2026-09-01 00:00:00", "2026-09-01 03:00:00"],
        "resolved_at": ["2026-09-01 02:00:00", None],
        "priority": ["P1 - Critical", "P1 - Critical"],
        "category": ["Network", "Network"],
        "assignment_group": ["Network-Ops", "Network-Ops"],
        "status": ["Closed", "In Progress"],
    })
    result = incident_gold_metrics(silver)
    assert result.loc[0, "incident_count"] == 2
    assert result.loc[0, "open_incident_count"] == 1
    assert result.loc[0, "avg_resolution_hours"] == 2.0
