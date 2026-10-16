"""Execute in a Fabric PySpark notebook with a default Lakehouse attached.

Single writer required. Source validity intervals use FHIR meta.lastUpdated;
ingestion timestamps describe when this system observed each resource version.
"""
import hashlib
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
import notebookutils


RESOURCES = ('Patient', 'Encounter', 'Observation', 'Condition')
BASE_COLUMNS = ['resource_type', 'resource_id', 'version_id', 'source_updated',
                'payload_hash', 'resource_json', 'extraction_timestamp',
                'saved_timestamp', 'api_url_or_params', 'raw_path', 'run_id']
ROW_SCHEMA = StructType([StructField(c, StringType(), True) for c in BASE_COLUMNS])


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def windows(start, end):
    first = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    last = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    if first >= last:
        raise ValueError('start_date must precede end_date (exclusive)')
    while first < last:
        nxt = min(first + timedelta(days=1), last)
        yield first.isoformat(), nxt.isoformat()
        first = nxt


def append_rows(spark, rows, table):
    if not rows:
        return
    df = spark.createDataFrame(rows, ROW_SCHEMA)
    for col in ['source_updated', 'extraction_timestamp', 'saved_timestamp']:
        df = df.withColumn(col, F.to_timestamp(col))
    # Same version with a different payload is retained for investigation.
    keys = ['resource_type', 'resource_id', 'version_id', 'payload_hash']
    df = df.dropDuplicates(keys)
    if spark.catalog.tableExists(table):
        match = ' AND '.join(f't.{k} <=> s.{k}' for k in keys)
        DeltaTable.forName(spark, table).alias('t').merge(df.alias('s'), match).whenNotMatchedInsertAll().execute()
    else:
        df.write.format('delta').mode('errorifexists').saveAsTable(table)


def audit(spark, event):
    # Explicit string schema also handles empty/error events without inference.
    fields = ['run_id', 'resource_type', 'window_start', 'window_end', 'api_url',
              'api_called_at', 'saved_at', 'raw_path', 'status', 'resource_count', 'error']
    schema = StructType([StructField(c, StringType(), True) for c in fields])
    spark.createDataFrame([{c: str(event[c]) if event.get(c) is not None else None
                            for c in fields}], schema).write.format('delta').mode('append').saveAsTable('ops_api_calls')


def ingest(spark, resource, start, end, base_url, page_size=200, max_pages=10000):
    if resource not in RESOURCES:
        raise ValueError('Unsupported FHIR resource')
    run_id = str(uuid.uuid4())
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=Retry(
        total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET'], respect_retry_after_header=True)))
    base = base_url.rstrip('/') + '/'
    source = urlparse(base)
    if source.scheme != 'https':
        raise ValueError('FHIR base URL must use HTTPS')
    for lower, upper in windows(start, end):
        endpoint = base + resource
        params = [('_lastUpdated', 'ge' + lower), ('_lastUpdated', 'lt' + upper),
                  ('_count', str(page_size)), ('_sort', '_lastUpdated'), ('_format', 'json')]
        url = requests.Request('GET', endpoint, params=params).prepare().url
        visited = set()
        page = 0
        while url:
            parsed = urlparse(url)
            # HAPI pagination may use /baseR4 without a trailing slash and
            # advertise HTTP behind its HTTPS reverse proxy. Upgrade only
            # the configured host; never follow another origin or base path.
            if parsed.scheme == 'http' and parsed.netloc == source.netloc:
                url = parsed._replace(scheme='https').geturl()
                parsed = urlparse(url)
            root_path = source.path.rstrip('/')
            if (parsed.scheme != 'https' or parsed.netloc != source.netloc or
                    not (parsed.path == root_path or parsed.path.startswith(root_path + '/'))):
                raise ValueError('Rejected pagination URL outside configured FHIR endpoint')
            if url in visited or page >= max_pages:
                raise RuntimeError('Pagination cycle or page limit; window is incomplete')
            visited.add(url)
            page += 1
            event = dict(run_id=run_id, resource_type=resource, window_start=lower,
                         window_end=upper, api_url=url, api_called_at=utcnow())
            try:
                response = session.get(url, headers={'Accept': 'application/fhir+json'}, timeout=(15, 120))
                response.raise_for_status()
                raw = response.text
                path = f'Files/raw/{resource}/date={lower[:10]}/run={run_id}/page-{page:06d}.json'
                notebookutils.fs.put(path, raw, True)
                event.update(raw_path=path, saved_at=utcnow())
                bundle = json.loads(raw)
                if bundle.get('resourceType') != 'Bundle':
                    raise ValueError('Expected a FHIR Bundle')
                entries = [e['resource'] for e in bundle.get('entry', [])
                           if e.get('search', {}).get('mode') != 'outcome' and e.get('resource', {}).get('resourceType') == resource]
                rows = []
                for item in entries:
                    metadata = item.get('meta', {})
                    if not item.get('id') or not metadata.get('lastUpdated'):
                        raise ValueError('Resource missing id or meta.lastUpdated; raw response retained')
                    canonical = json.dumps(item, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
                    rows.append(dict(resource_type=resource, resource_id=item['id'],
                        version_id=metadata.get('versionId'), source_updated=metadata['lastUpdated'],
                        payload_hash=hashlib.sha256(canonical.encode()).hexdigest(), resource_json=canonical,
                        extraction_timestamp=event['api_called_at'], saved_timestamp=event['saved_at'],
                        api_url_or_params=url, raw_path=path, run_id=run_id))
                append_rows(spark, rows, 'bronze_' + resource.lower())
                event.update(status='SUCCESS', resource_count=len(rows))
                audit(spark, event)
                next_links = [link['url'] for link in bundle.get('link', []) if link.get('relation') == 'next']
                url = urljoin(url, next_links[0]) if next_links else None
            except Exception as exc:
                event.update(status='FAILED', error=str(exc)[:4000])
                audit(spark, event)
                raise
        # Commit only after every page has been persisted. Failed windows are retried.
        spark.createDataFrame([(resource, lower, upper, run_id, utcnow())],
            'resource_type string, window_start string, window_end string, run_id string, completed_at string'
        ).write.format('delta').mode('append').saveAsTable('ops_completed_windows')
    session.close()


def transform(spark):
    for resource in RESOURCES:
        name = resource.lower()
        bronze = 'bronze_' + name
        if not spark.catalog.tableExists(bronze):
            # Empty valid API windows still produce typed, queryable tables.
            empty = spark.createDataFrame([], ROW_SCHEMA)
            for col in ['source_updated', 'extraction_timestamp', 'saved_timestamp']:
                empty = empty.withColumn(col, F.to_timestamp(col))
            empty.write.format('delta').mode('errorifexists').saveAsTable(bronze)
        df = spark.table(bronze)
        conflict = df.groupBy('resource_id', 'source_updated').agg(
            F.countDistinct('payload_hash').alias('payloads')).filter('payloads > 1').limit(1).count()
        if conflict:
            raise RuntimeError(f'{resource}: conflicting payloads at identical source time; inspect Bronze')
        dedup = Window.partitionBy('resource_id', 'source_updated', 'payload_hash').orderBy('extraction_timestamp')
        df = df.withColumn('_rn', F.row_number().over(dedup)).filter('_rn = 1').drop('_rn')
        ordered = Window.partitionBy('resource_id').orderBy('source_updated')
        # Remove unchanged source updates; retain true changes, including A -> B -> A.
        df = df.withColumn('_previous_hash', F.lag('payload_hash').over(ordered))
        df = df.filter(F.col('_previous_hash').isNull() | (F.col('payload_hash') != F.col('_previous_hash'))).drop('_previous_hash')
        df = (df.withColumn('valid_from', F.col('source_updated'))
              .withColumn('valid_to', F.lead('source_updated').over(ordered))
              .withColumn('is_current', F.col('valid_to').isNull())
              .withColumn('silver_processed_at', F.current_timestamp()))
        fields = {
            'patient_reference': '$.subject.reference', 'encounter_reference': '$.encounter.reference',
            'clinical_status': '$.clinicalStatus.coding[0].code', 'status': '$.status',
            'code': '$.code.coding[0].code', 'code_system': '$.code.coding[0].system',
            'code_display': '$.code.coding[0].display', 'gender': '$.gender', 'birth_date': '$.birthDate',
            'encounter_class': '$.class.code', 'period_start': '$.period.start', 'period_end': '$.period.end',
            'effective_at': '$.effectiveDateTime', 'quantity_value': '$.valueQuantity.value',
            'quantity_unit': '$.valueQuantity.unit', 'onset_at': '$.onsetDateTime'}
        for col, path in fields.items():
            df = df.withColumn(col, F.get_json_object('resource_json', path))
        # Local and absolute references only; contained and identifier references remain unresolved.
        for col in ['patient_reference', 'encounter_reference']:
            kind = 'Patient' if col == 'patient_reference' else 'Encounter'
            df = df.withColumn(col.replace('_reference', '_id'), F.regexp_extract(
                F.col(col), rf'(?:^|/){kind}/([^/]+)(?:/_history/[^/]+)?$', 1))
        df = df.withColumn('quantity_value', F.col('quantity_value').cast('double'))
        df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('silver_' + name)
        current = df.filter('is_current').withColumn('gold_processed_at', F.current_timestamp())
        current.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('gold_' + name)
    spark.sql('''SELECT e.resource_id AS encounter_id, e.patient_id, e.status,
        e.encounter_class, e.period_start, e.period_end,
        p.gender, p.birth_date, e.extraction_timestamp, e.version_id
        FROM gold_encounter e LEFT JOIN gold_patient p ON e.patient_id = p.resource_id'''
    ).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('gold_encounter_analytics')
    for resource in RESOURCES:
        invalid = spark.table('silver_' + resource.lower()).groupBy('resource_id').agg(
            F.sum(F.col('is_current').cast('int')).alias('currents')).filter('currents <> 1').count()
        if invalid:
            raise RuntimeError(f'{resource}: SCD2 current-row validation failed')


def run(spark, start_date, end_date, resource_type='ALL',
        base_url='https://hapi.fhir.org/baseR4', page_size=200, max_pages=10000):
    if resource_type == 'TRANSFORM':
        transform(spark)
        return
    if resource_type != 'ALL' and resource_type not in RESOURCES:
        raise ValueError('Invalid resource_type')
    for resource in RESOURCES if resource_type == 'ALL' else [resource_type]:
        ingest(spark, resource, start_date, end_date, base_url, int(page_size), int(max_pages))
    if resource_type == 'ALL':
        transform(spark)
