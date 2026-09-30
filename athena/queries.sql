-- Example Athena queries against the incident_lake Glue database
-- (workgroup: incident-lake; see infrastructure/incident-lake.yaml).
--
-- Only the Gold table is currently cataloged: src/transforms.incident_gold_metrics
-- writes columns opened_date, priority, category, assignment_group,
-- incident_count, open_incident_count, avg_resolution_hours to
-- s3://<LAKE_BUCKET>/gold/incident_daily_metrics/, matching
-- incident_lake.incident_daily_metrics below.

SHOW TABLES IN incident_lake;

-- Daily incident volume, open backlog, and resolution time -- the table
-- QuickSight/Q reads directly, no further grouping needed.
SELECT opened_date, priority, category, assignment_group,
       incident_count, open_incident_count, avg_resolution_hours
FROM incident_lake.incident_daily_metrics
ORDER BY opened_date DESC;

-- P1/P2 resolution-time trend, for an SLA dashboard panel.
SELECT opened_date, priority, avg_resolution_hours
FROM incident_lake.incident_daily_metrics
WHERE priority IN ('P1 - Critical', 'P2 - High')
ORDER BY opened_date;

-- Open backlog by assignment group, most recent day.
SELECT assignment_group, SUM(open_incident_count) AS open_incidents
FROM incident_lake.incident_daily_metrics
WHERE opened_date = (SELECT MAX(opened_date) FROM incident_lake.incident_daily_metrics)
GROUP BY assignment_group
ORDER BY open_incidents DESC;
