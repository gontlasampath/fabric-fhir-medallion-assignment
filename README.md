# FHIR ingestion and analytics for Microsoft Fabric

Status: deployed and verified in Microsoft Fabric. The three-day UTC backfill
(2026-10-13 inclusive to 2026-10-16 exclusive) and five-activity pipeline replay
both succeeded. Bronze/Silver/Gold tables, SQL views, semantic model, one-page
Power BI report, and live-page dashboard are saved in the workspace.

[Open dashboard](https://app.fabric.microsoft.com/groups/9a1e4d7e-ae06-4011-b648-ccb54d690d3a/dashboards/ceb9f909-3e7e-4a3d-9057-a8cb7254da48?experience=power-bi)

Verified current counts: Patient 726; Encounter 3,761; Observation 1,726;
Condition 1,853. SQL SCD2 validation returned zero invalid resources.
See `deployment-status.json` and `evidence/` for execution records.
The trial capacity produced transient compute-limit retries; the configured
retry policy recovered and the final pipeline status was Succeeded.

## Deployment

1. Sign in to the intended giftzee.online Microsoft tenant. Use an existing
   Fabric-capable workspace and capacity. Creating a paid capacity is not part of
   this package.
2. Create a Lakehouse and import `notebooks/FHIR_Medallion.ipynb`. Attach the
   Lakehouse as its default. The code uses unqualified tables in the default
   schema, compatible with a schema-enabled Lakehouse's dbo default.
3. Confirm the notebook's first code cell is marked as the parameter cell.
   Set `start_date` and `end_date` to a three-day UTC range, exclusive end.
   Defaults use the three completed UTC days before execution.
4. Run the notebook with `resource_type=ALL` for a first complete load.
5. Create a Fabric Data Pipeline with five Notebook activities pointing to this
   same notebook. Chain them with Succeeded dependencies:
   Patient -> Encounter -> Observation -> Condition -> TRANSFORM.
   Pass `resource_type` for each activity and matching `start_date`, `end_date`
   pipeline parameters. `pipeline.template.json` is a deployment template with
   unresolved notebook/workspace IDs; validate its definition against Fabric
   before deploying. It is not a deployed pipeline.
6. On subsequent daily executions, pass yesterday's UTC start and today's UTC
   exclusive end. Replay a small overlap window to catch delayed updates.
   `ops_completed_windows` records complete resource windows; the caller chooses
   the next window. It is not an automatically advanced scheduler watermark.
7. After table synchronization, execute `sql/gold_views.sql` in the Lakehouse SQL
   analytics endpoint. These T-SQL views are persistent reporting objects.
8. Verify run history, API audit records, table counts, current-row invariants,
   and view queries in Fabric. Export screenshots and query results as execution
   evidence. A configured three-day backfill is distinct from three daily runs.

## Storage and lineage

- Raw: `Files/raw/<Resource>/date=YYYY-MM-DD/run=<UUID>/page-NNNNNN.json`.
  Response text is persisted before parsing or table conversion.
- Bronze: `bronze_patient`, `bronze_encounter`, `bronze_observation`,
  `bronze_condition`. Spark converts JSON resources to Delta records with the
  original resource JSON, source version, payload hash, API request URL,
  extraction/save timestamps, run ID, and raw-file pointer.
- Silver: corresponding `silver_*` Delta tables, cleaned and deduplicated,
  with selected typed analytics fields and SCD2 source-time validity intervals.
  Intervals are half open: valid_from <= time < valid_to; NULL end is current.
- Gold: corresponding `gold_*` current records and `gold_encounter_analytics`,
  which joins Encounter.patient_id to Patient.resource_id using a left join.
  Observation and Condition have patient_id and encounter_id relationships to
  the corresponding current tables. References are not enforced foreign keys.
- Ops: `ops_api_calls` logs page calls, persistence times, counts, failures, and
  raw locations. `ops_completed_windows` records fully persisted windows.

All layers retain metadata. Gold retains the selected current source version;
Silver retains observed change history. Gold and Silver processing timestamps
describe transformation time. Bronze Delta history provides table commit history.

## Correctness and operational boundaries

FHIR search uses repeated `_lastUpdated` parameters for inclusive lower and
exclusive upper bounds. Pagination follows Bundle `next` links verbatim,
restricted to the configured HTTPS endpoint. Retries honor throttling and a page
limit fails the window explicitly. Never treat a capped or failed run as complete.

Bronze merges deduplicate resource/version/hash, making repeated windows safe.
Raw and audit retain every attempt. Silver is recomputed from accumulated Bronze
to accommodate out-of-order observations. Unchanged payloads do not create new
SCD2 intervals. Conflicting payloads at the same source timestamp fail for manual
investigation rather than silently choosing a record. Only one pipeline writer
may run at a time; overwrites across multiple tables are not a single transaction.
Gold should be consumed only after the entire pipeline succeeds.

FHIR ordinary search returns the current matching resource, not every historic
version or deleted resource. History tracks versions this project actually
observes; a backfill cannot reconstruct versions overwritten before collection.
Capturing every source change requires a supported FHIR history/change feed.
No deletion detection is claimed. Public test-server data may change or contain
unresolved references. No sensitive production source is configured.

Patient demographics, Encounter class/period, Observation code/quantity, and
Condition code/status/onset are extracted. Non-quantity Observation values and
other FHIR polymorphic fields remain available in resource_json. Mixed units
must not be averaged without conversion. Contained/identifier-only references
remain unresolved. The optional Power BI report is implemented; no XML extension is included.

## Validation and further testing

- Completed: four resources across three UTC days, with multiple-page windows
  verified in the API audit query.
- Completed: successful pipeline replay, stable Gold counts, and new audit/raw
  attempt records. Bronze version/hash merges make the replay idempotent.
- Use controlled fixtures to validate A -> B -> A history, late arrivals, empty
  windows, HTTP failures, pagination failure, and conflicting source versions.
- Completed: SQL checked exactly one current row per resource ID, positive
  validity intervals, and no overlaps; zero invalid resources.
- Completed: pipeline ordering, successful status, SQL counts, report, and
  dashboard evidence captured. Controlled failure fixtures above remain additional
  regression testing; they are not claimed as executed.

## References

- Assignment source: D:/Downloads/Fabric Data Assignment - Pragati Jha.pdf
- FHIR source: https://hapi.fhir.org/baseR4/swagger-ui/
- Fabric notebooks: https://learn.microsoft.com/en-us/fabric/data-engineering/author-execute-notebook
- Notebook orchestration: https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-notebook-run

Build/rebuild the notebook and submission ZIP with `python build_package.py`.

## Reporting model

`model.tmdl` defines five DAX measures and four Patient relationships. It was
applied to the saved Fabric Direct Lake semantic model. The report uses Gold
counts, Patient gender distribution, and Encounter class distribution. The
dashboard pins the full report page as a live tile. The automatically created
model includes Bronze/Silver/Ops tables too; report visuals use Gold tables.
