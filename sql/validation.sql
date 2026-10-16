-- Gold row counts and SCD2 current/history row counts.
SELECT 'Patient' AS resource_type, COUNT_BIG(*) AS current_rows FROM dbo.gold_patient
UNION ALL SELECT 'Encounter', COUNT_BIG(*) FROM dbo.gold_encounter
UNION ALL SELECT 'Observation', COUNT_BIG(*) FROM dbo.gold_observation
UNION ALL SELECT 'Condition', COUNT_BIG(*) FROM dbo.gold_condition;

-- Latest fully committed attempt for each resource/day; pages > 1 proves pagination.
WITH latest AS (
 SELECT *, ROW_NUMBER() OVER (
  PARTITION BY resource_type, window_start ORDER BY completed_at DESC
 ) AS rn FROM dbo.ops_completed_windows
)
SELECT w.resource_type, LEFT(w.window_start,10) AS load_day,
 COUNT(a.api_url) AS successful_pages,
 SUM(TRY_CAST(a.resource_count AS bigint)) AS returned_resources
FROM latest w
LEFT JOIN dbo.ops_api_calls a ON a.run_id=w.run_id
 AND a.resource_type=w.resource_type AND a.window_start=w.window_start
 AND a.status='SUCCESS'
WHERE w.rn=1
GROUP BY w.resource_type,LEFT(w.window_start,10)
ORDER BY w.resource_type,load_day;

-- Must return zero invalid resources for every table.
WITH history AS (
 SELECT 'Patient' AS resource_type, resource_id, valid_from,valid_to,is_current FROM dbo.silver_patient
 UNION ALL SELECT 'Encounter',resource_id,valid_from,valid_to,is_current FROM dbo.silver_encounter
 UNION ALL SELECT 'Observation',resource_id,valid_from,valid_to,is_current FROM dbo.silver_observation
 UNION ALL SELECT 'Condition',resource_id,valid_from,valid_to,is_current FROM dbo.silver_condition
), ordered AS (
 SELECT *,LEAD(valid_from) OVER(PARTITION BY resource_type,resource_id ORDER BY valid_from) AS next_start
 FROM history
), invalid AS (
 SELECT resource_type,resource_id FROM ordered
 GROUP BY resource_type,resource_id
 HAVING SUM(CASE WHEN is_current=1 THEN 1 ELSE 0 END)<>1
 OR SUM(CASE WHEN valid_to IS NOT NULL AND valid_to<=valid_from THEN 1 ELSE 0 END)>0
 OR SUM(CASE WHEN next_start IS NOT NULL AND (valid_to IS NULL OR valid_to>next_start) THEN 1 ELSE 0 END)>0
)
SELECT COUNT_BIG(*) AS invalid_scd2_resources FROM invalid;
