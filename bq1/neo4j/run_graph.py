#!/usr/bin/env python3
"""Role 4 EC2 runner. Python 3.9+ standard library + AWS CLI; no pip packages.

Run on the existing Neo4j EC2 host, NOT on a personal laptop.
Exports fresh Athena inputs, reconciles, imports a new scope, runs GDS and
publishes complete CSVs before success.json. Never starts/stops EC2 itself.
"""
import argparse
import base64
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

ROOT = Path(__file__).resolve().parent
FEATURE_HEADER = ['bank_id', 'betweenness', 'community_id']
TX_HEADER = ['txn_id', 'from_bank', 'from_account', 'to_bank', 'to_account',
             'txn_ts', 'amount_paid_usd', 'attempt_id', 'pattern_type']
BANK_HEADER = ['from_bank', 'to_bank', 'laundering_txns', 'laundering_usd']


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def statements(path):
    # These checked-in files have full-line comments and no semicolons in literals.
    lines = [line for line in path.read_text(encoding='utf-8-sig').splitlines()
             if not line.lstrip().startswith(('--', '//'))]
    return [s.strip() for s in '\n'.join(lines).split(';') if s.strip()]


def aws(args, region):
    result = subprocess.run(['aws', *args, '--region', region],
                            capture_output=True, text=True, timeout=180)
    # Avoid echoing whole arguments/queries or local credential data in logs.
    require(result.returncode == 0, 'AWS CLI failed: ' + ' '.join(args[:2]))
    return result.stdout


def aws_json(args, region):
    return json.loads(aws([*args, '--output', 'json'], region))


def query_athena(sql, args, work, download=True):
    query = aws_json(['athena', 'start-query-execution', '--query-string', sql,
        '--work-group', args.workgroup, '--query-execution-context',
        json.dumps({'Database': 'is459_g2t1_aml'}), '--result-configuration',
        json.dumps({'OutputLocation': args.athena_output}),
        '--result-reuse-configuration',
        json.dumps({'ResultReuseByAgeConfiguration': {'Enabled': False}})], args.region)
    query_id = query['QueryExecutionId']
    deadline = time.monotonic() + args.query_timeout
    while time.monotonic() < deadline:
        execution = aws_json(['athena', 'get-query-execution',
                              '--query-execution-id', query_id], args.region)['QueryExecution']
        state = execution['Status']['State']
        if state == 'SUCCEEDED':
            if not download:
                return query_id, None
            path = work / (query_id + '.csv')
            aws(['s3', 'cp', execution['ResultConfiguration']['OutputLocation'],
                 str(path), '--only-show-errors'], args.region)
            return query_id, path
        require(state not in ('FAILED', 'CANCELLED'), 'Athena query failed: ' + query_id)
        time.sleep(3)
    aws(['athena', 'stop-query-execution', '--query-execution-id', query_id], args.region)
    raise RuntimeError('Athena timeout: ' + query_id)


def rows(path, expected_header=None):
    with path.open(newline='', encoding='utf-8-sig') as handle:
        reader = csv.DictReader(handle)
        if expected_header:
            require(reader.fieldnames == expected_header, 'Unexpected CSV header: ' + path.name)
        result = list(reader)
    require(all(None not in row and all(v is not None for v in row.values())
                for row in result), 'Malformed CSV: ' + path.name)
    return result


def check_inputs(bank_rows, tx_rows, baseline):
    require(tx_rows, 'Empty graph population')
    seen = set()
    for row in tx_rows:
        require(re.fullmatch(r'-?\d+', row['txn_id']) is not None, 'Invalid transaction ID')
        require(row['txn_id'] not in seen, 'Duplicate transaction ID')
        seen.add(row['txn_id'])
        for name in ('from_bank', 'to_bank', 'from_account', 'to_account'):
            require(bool(row[name].strip()), 'Missing graph key: ' + name)
        require(re.fullmatch(r'\d+', row['from_bank']) is not None
                and re.fullmatch(r'\d+', row['to_bank']) is not None, 'Invalid bank ID')
        stamp = datetime.fromisoformat(row['txn_ts'])
        require(stamp.tzinfo is None, 'Unexpected timestamp timezone')
        require(Decimal(row['amount_paid_usd']).is_finite()
                and Decimal(row['amount_paid_usd']) < Decimal('20000000'),
                'Unexpected amount in filtered population')
    expected = {}
    for row in tx_rows:
        if row['from_bank'] != row['to_bank']:
            key = (row['from_bank'], row['to_bank'])
            count, value = expected.get(key, (0, Decimal(0)))
            expected[key] = (count + 1, value + Decimal(row['amount_paid_usd']))
    require(len(bank_rows) == len(expected), 'Bank-pair count does not match transactions')
    bank_keys = set()
    for row in bank_rows:
        key = (row['from_bank'], row['to_bank'])
        require(key not in bank_keys and key in expected, 'Duplicate/unexpected bank pair')
        bank_keys.add(key)
        count, value = expected[key]
        require(int(row['laundering_txns']) == count, 'Per-pair transaction count mismatch')
        require(abs(Decimal(row['laundering_usd']) - value) <= Decimal('.01'),
                'Per-pair USD mismatch')
    totals = {
        'evidence_transactions': len(tx_rows),
        'evidence_usd': str(sum((Decimal(r['amount_paid_usd']) for r in tx_rows), Decimal(0))),
        'cross_bank_transactions': sum(v[0] for v in expected.values()),
        'cross_bank_usd': str(sum((v[1] for v in expected.values()), Decimal(0))),
        'corridors': len(bank_rows),
        'banks': len({b for pair in expected for b in pair}),
    }
    for name in ('evidence_transactions', 'cross_bank_transactions'):
        require(totals[name] == int(baseline[name]), 'Source/export count mismatch: ' + name)
    for name in ('evidence_usd', 'cross_bank_usd'):
        require(abs(Decimal(totals[name]) - Decimal(baseline[name])) <= Decimal('.01'),
                'Source/export USD mismatch: ' + name)
    return totals


class Neo4j:
    def __init__(self, env_path, timeout):
        auth = None
        for line in env_path.read_text(encoding='utf-8').splitlines():
            if line.startswith('NEO4J_AUTH='):
                auth = line.partition('=')[2].strip()
                if len(auth) >= 2 and auth[0] == auth[-1] and auth[0] in ('"', "'"):
                    auth = auth[1:-1]
        require(auth is not None and '/' in auth and auth != 'none',
                'Expected NEO4J_AUTH=user/password in protected instance-local .env')
        user, password = auth.split('/', 1)
        self.header = 'Basic ' + base64.b64encode((user + ':' + password).encode()).decode()
        self.timeout = timeout

    def query(self, text, params=None):
        payload = json.dumps({'statements': [{'statement': text,
            'parameters': params or {}, 'resultDataContents': ['row']}]}).encode()
        request = urllib.request.Request('http://127.0.0.1:7474/db/neo4j/tx/commit',
            data=payload, headers={'Authorization': self.header, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            data = json.load(response)
        require(not data.get('errors'), 'Neo4j statement failed: ' +
                ','.join(e.get('code', '') for e in data.get('errors', [])))
        result = data['results'][0]
        return [dict(zip(result['columns'], r['row'])) for r in result['data']]


def validate_features(features, totals):
    require(len(features) == totals['banks'], 'Incomplete feature export')
    require(len({r['bank_id'] for r in features}) == len(features), 'Duplicate bank features')
    for row in features:
        require(row['bank_id'] is not None and row['community_id'] is not None
                and isinstance(row['community_id'], int), 'Missing/invalid community')
        require(isinstance(row['betweenness'], (float, int))
                and math.isfinite(row['betweenness']) and row['betweenness'] >= 0,
                'Missing/invalid betweenness')


def write_csv(path, records, header):
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(records)


def publish(args, path, key):
    aws(['s3', 'cp', str(path), 's3://' + args.bucket + '/' + key,
         '--only-show-errors'], args.region)


def run(args):
    # Root is expected for SSM to read the mounted import dir and local .env.
    import fcntl  # Linux EC2 only; keep pure validation helpers testable on Windows.
    home = Path(args.role4_home)
    require(home.is_dir(), 'Role 4 EC2 home is missing')
    with (home / '.pipeline.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another Role 4 pipeline is running')
        # Immutable run prefixes; never overwrite accepted hi-01 or retry partial runs.
        for prefix in ('bq1_graph_edges/', 'bq1_graph_results/'):
            existing = aws_json(['s3api', 'list-objects-v2', '--bucket', args.bucket,
                '--prefix', prefix + args.run_id + '/', '--max-keys', '1'], args.region)
            require(not existing.get('Contents'), 'Run prefix exists; choose a NEW run ID')
        work = home / 'pipeline-runs' / args.run_id
        work.mkdir(parents=True, exist_ok=False)
        # Auth/readiness checked before issuing any Athena exports.
        db = Neo4j(home / '.env', args.query_timeout)
        versions = db.query('CALL dbms.components() YIELD versions,edition RETURN versions,edition')
        gds = db.query('RETURN gds.version() AS version')
        exports = statements(ROOT / 'export_edges.sql')
        require(len(exports) == 3, 'Expected exactly three export/source SQL statements')
        exports = [s.replace("'HI-Medium'", "'" + args.dataset + "'") for s in exports]
        query_ids = {}
        paths = {}
        # Query baseline first. Sonia must serialize source refreshes around this run.
        query_ids['source'], baseline_path = query_athena(exports[2], args, work)
        baseline_rows = rows(baseline_path)
        require(len(baseline_rows) == 1, 'Invalid source summary')
        for key, sql in zip(('bank_edges', 'transactions'), exports[:2]):
            query_ids[key], paths[key] = query_athena(sql, args, work)
        totals = check_inputs(rows(paths['bank_edges'], BANK_HEADER),
                              rows(paths['transactions'], TX_HEADER), baseline_rows[0])
        # Input files live below the existing dedicated Docker bind mount.
        import_dir = home / 'import' / ('auto-' + args.run_id)
        import_dir.mkdir(mode=0o755, exist_ok=False)
        manifest = {'schema_version': 1, 'run_id': args.run_id, 'dataset': args.dataset,
                    'filter': 'is_laundering=1 AND amount_paid_usd<20000000',
                    'query_ids': query_ids, 'totals': totals, 'files': {}}
        for key, source in paths.items():
            content = source.read_bytes()
            target = import_dir / (key + '.csv')
            target.write_bytes(content)
            target.chmod(0o644)
            manifest['files'][key] = {'sha256': hashlib.sha256(content).hexdigest(),
                                     'key': 'bq1_graph_edges/' + args.run_id + '/' + key + '.csv'}
            publish(args, target, manifest['files'][key]['key'])
        manifest_path = work / 'manifest.json'
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
        publish(args, manifest_path, 'bq1_graph_edges/' + args.run_id + '/manifest.json')
        imports = statements(ROOT / 'import.cypher')
        for statement in imports:
            statement = statement.replace('file:///hi-01/',
                                           'file:///auto-' + args.run_id + '/')
            db.query(statement, {'scope': args.run_id})
        # Independently verify active scope, graph count/value reconciliation.
        for label in ('ActiveBank', 'ActiveAccount'):
            scopes = db.query('MATCH (n:' + label + ') RETURN collect(DISTINCT n.scope) AS scopes')
            require(scopes[0]['scopes'] == [args.run_id], 'Mixed active graph scopes')
        actual = db.query(imports[-1], {'scope': args.run_id})[0]
        for name in ('corridors', 'cross_bank_transactions', 'evidence_transactions'):
            require(actual[name] == totals[name], 'Import count mismatch: ' + name)
        for name in ('cross_bank_usd', 'evidence_usd'):
            require(abs(Decimal(str(actual[name])) - Decimal(totals[name])) <= Decimal('.01'),
                    'Import USD mismatch: ' + name)
        projection = db.query("CALL gds.graph.project('role4Directed','ActiveBank',"
                              "{LAUNDERING_TO:{orientation:'NATURAL'}}) "
                              'YIELD nodeCount,relationshipCount RETURN nodeCount,relationshipCount')[0]
        require(projection['nodeCount'] == totals['banks']
                and projection['relationshipCount'] == totals['corridors'], 'Wrong projection size')
        budget = args.memory_budget_mib * 1024 * 1024
        config = "{writeProperty:'betweenness',concurrency:2}"
        estimate = db.query("CALL gds.betweenness.write.estimate('role4Directed'," + config +
                            ') YIELD bytesMax RETURN bytesMax')[0]
        require(estimate['bytesMax'] <= budget, 'Betweenness exceeds algorithm memory budget')
        between = db.query("CALL gds.betweenness.write('role4Directed'," + config +
                           ') YIELD nodePropertiesWritten RETURN nodePropertiesWritten')[0]
        require(between['nodePropertiesWritten'] == totals['banks'], 'Incomplete betweenness write')
        db.query("MATCH (a:ActiveBank)-[r:LAUNDERING_TO]->(b:ActiveBank) "
                 'WITH CASE WHEN a.bank_id<b.bank_id THEN a ELSE b END AS lo, '
                 'CASE WHEN a.bank_id<b.bank_id THEN b ELSE a END AS hi, '
                 'sum(r.laundering_txns) AS weight '
                 'MERGE (lo)-[u:BANK_LINK]->(hi) SET u.weight=toFloat(weight)')
        db.query("CALL gds.graph.project('role4Community','ActiveBank',"
                 "{BANK_LINK:{orientation:'UNDIRECTED',properties:'weight'}})")
        config = "{writeProperty:'community_id',relationshipWeightProperty:'weight',concurrency:1}"
        community_estimate = db.query("CALL gds.louvain.write.estimate('role4Community'," + config +
                                      ') YIELD bytesMax RETURN bytesMax')[0]
        require(community_estimate['bytesMax'] <= budget, 'Louvain exceeds algorithm memory budget')
        community = db.query("CALL gds.louvain.write('role4Community'," + config +
            ') YIELD communityCount,modularity,nodePropertiesWritten '
            'RETURN communityCount,modularity,nodePropertiesWritten')[0]
        require(community['nodePropertiesWritten'] == totals['banks'], 'Incomplete Louvain write')
        features = db.query('MATCH (b:ActiveBank) RETURN b.bank_id AS bank_id, '
            'b.betweenness AS betweenness,b.community_id AS community_id ORDER BY bank_id')
        validate_features(features, totals)
        require(len({r['community_id'] for r in features}) == community['communityCount'],
                'Community coverage mismatch')
        sizes = db.query('MATCH (b:ActiveBank) RETURN b.community_id AS community_id,'
            'count(*) AS bank_count ORDER BY bank_count DESC,community_id')
        require(sum(r['bank_count'] for r in sizes) == totals['banks'], 'Community size mismatch')
        feature_path = work / 'bank_features.csv'
        write_csv(feature_path, features, FEATURE_HEADER)
        require(len(rows(feature_path, FEATURE_HEADER)) == totals['banks'], 'Output CSV truncated')
        size_path = work / 'community_sizes.csv'
        write_csv(size_path, sizes, ['community_id', 'bank_count'])
        output_prefix = 'bq1_graph_results/' + args.run_id + '/'
        publish(args, feature_path, output_prefix + 'bank_features/bank_features.csv')
        publish(args, size_path, output_prefix + 'community_sizes/community_sizes.csv')
        # Register a distinct table for this new run. No destructive DROP/CTAS.
        table = 'bq1_graph_bank_features_' + args.run_id.replace('-', '_')
        location = 's3://' + args.bucket + '/' + output_prefix + 'bank_features/'
        ddl = "CREATE EXTERNAL TABLE is459_g2t1_aml." + table + (
            " (bank_id string,betweenness double,community_id bigint) "
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' "
            "WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"') "
            "STORED AS TEXTFILE LOCATION '" + location +
            "' TBLPROPERTIES ('skip.header.line.count'='1')")
        query_ids['register'], _ = query_athena(ddl, args, work, download=False)
        validate_sql = ('SELECT count(*) AS banks,count(DISTINCT bank_id) AS unique_banks,'
            'count(DISTINCT community_id) AS communities,'
            'count_if(bank_id IS NULL OR betweenness IS NULL OR community_id IS NULL) AS bad '
            'FROM is459_g2t1_aml.' + table)
        query_ids['validate'], check_path = query_athena(validate_sql, args, work)
        check = rows(check_path)[0]
        require(int(check['banks']) == totals['banks']
                and int(check['unique_banks']) == totals['banks']
                and int(check['communities']) == community['communityCount']
                and int(check['bad']) == 0, 'Published Athena feature validation failed')
        receipt = {**manifest, 'status': 'SUCCEEDED',
            'completed_utc': datetime.now(timezone.utc).isoformat(),
            'feature_table': 'is459_g2t1_aml.' + table,
            'feature_location': location, 'neo4j': versions, 'gds': gds,
            'betweenness_estimate': estimate, 'louvain_estimate': community_estimate,
            'louvain': community,
            'code_sha256': {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                for name in ('run_graph.py', 'export_edges.sql', 'import.cypher')},
            'feature_sha256': hashlib.sha256(feature_path.read_bytes()).hexdigest()}
        receipt_path = work / 'success.json'
        receipt_path.write_text(json.dumps(receipt, indent=2), encoding='utf-8')
        publish(args, receipt_path, output_prefix + 'success.json')  # publish LAST
        print(json.dumps({'status': 'SUCCEEDED', 'run_id': args.run_id,
                          'feature_table': receipt['feature_table']}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--dataset', choices=['sample', 'HI-Medium', 'LI-Medium'], required=True)
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--bucket', default='is459-g2t1-aml-processed-bucket')
    parser.add_argument('--workgroup', default='primary')
    parser.add_argument('--athena-output', default='s3://is459-g2t1-aml-athena-bucket/results/')
    parser.add_argument('--role4-home', default='/home/ssm-user/role4')
    parser.add_argument('--query-timeout', type=int, default=1200)
    parser.add_argument('--memory-budget-mib', type=int, default=128)
    args = parser.parse_args()
    require(re.fullmatch(r'[a-z][a-z0-9-]{0,39}', args.run_id) is not None,
            'Run ID must be 1–40 lowercase letters/digits/hyphens starting with a letter')
    require(args.query_timeout > 0 and args.memory_budget_mib > 0, 'Invalid resource limit')
    run(args)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Never print credentials or an entire environment in a failure log.
        print('FAILED: ' + str(exc), file=sys.stderr)
        sys.exit(1)
