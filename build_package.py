"""Build a self-contained Fabric-importable notebook and parameterized pipeline template."""
import ast
import json
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

root = Path(__file__).parent
source = (root / 'src/fhir_medallion.py').read_text(encoding='utf-8')
ast.parse(source)
parameters = '''# Set these in Fabric or pass matching pipeline base parameters.
from datetime import datetime, timedelta, timezone
end_date = datetime.now(timezone.utc).date().isoformat()
start_date = (datetime.now(timezone.utc).date() - timedelta(days=3)).isoformat()
resource_type = "ALL"
base_url = "https://hapi.fhir.org/baseR4"
page_size = 200
max_pages = 10000
'''
def code(text, tags=None):
    return {'cell_type': 'code', 'metadata': {'tags': tags or []},
            'execution_count': None, 'outputs': [], 'source': text.splitlines(True)}
notebook = {'nbformat': 4, 'nbformat_minor': 5,
    'metadata': {'kernelspec': {'name': 'synapse_pyspark', 'display_name': 'Synapse PySpark', 'language': 'python'},
                 'language_info': {'name': 'python'}},
    'cells': [{'cell_type': 'markdown', 'metadata': {}, 'source': [
        '# FHIR medallion ingestion\nAttach a default Fabric Lakehouse before running. '
        'Dates are UTC, start inclusive and end exclusive. Run with one writer only.\n']},
        code(parameters, ['parameters']), code(source),
        code('run(spark, start_date, end_date, resource_type, base_url, page_size, max_pages)\n')]}
(root / 'notebooks/FHIR_Medallion.ipynb').write_text(json.dumps(notebook, indent=2), encoding='utf-8')
activities = []
previous = None
for resource in ['Patient', 'Encounter', 'Observation', 'Condition', 'TRANSFORM']:
    name = 'Transform' if resource == 'TRANSFORM' else 'Ingest_' + resource
    activities.append({'name': name, 'type': 'TridentNotebook',
        'dependsOn': [] if previous is None else [{'activity': previous, 'dependencyConditions': ['Succeeded']}],
        'policy': {'timeout': '0.12:00:00', 'retry': 2, 'retryIntervalInSeconds': 60},
        'typeProperties': {'notebookId': '<NOTEBOOK_ID>', 'workspaceId': '<WORKSPACE_ID>',
            'parameters': {'resource_type': {'value': resource, 'type': 'string'},
                'start_date': {'value': {'value': '@pipeline().parameters.start_date', 'type': 'Expression'}, 'type': 'string'},
                'end_date': {'value': {'value': '@pipeline().parameters.end_date', 'type': 'Expression'}, 'type': 'string'}}}})
    previous = name
pipeline = {'properties': {'activities': activities, 'parameters': {
    'start_date': {'type': 'string'}, 'end_date': {'type': 'string'}}}}
(root / 'pipeline.template.json').write_text(json.dumps(pipeline, indent=2), encoding='utf-8')
with ZipFile(root.parent / 'fabric-fhir-project.zip', 'w', ZIP_DEFLATED) as archive:
    for file in sorted(root.rglob('*')):
        if file.is_file() and '__pycache__' not in str(file):
            archive.write(file, file.relative_to(root.parent))
print('Notebook and ZIP built; Python syntax verified. See deployment-status.json for cloud execution results.')
