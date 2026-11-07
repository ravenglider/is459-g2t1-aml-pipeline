# Role 4 EC2 and Neo4j setup record

This records the shared server configuration used for the validated manual sample and HI runs. Existing installation is complete; do not recreate containers or credentials merely to read these notes. The personal step-by-step walkthrough stays local and is excluded from GitHub; this setup record accompanies the project source.

## Current automation status - Task 11 complete, 6 November 2026

Readiness, verified release deployment, direct sample/HI execution, independent
Athena output checks, manual/automated comparison, fresh-ID HI repeat and earlier
graph preservation all passed. Observed wall times: sample37.155682s,
HI run01 63.522618s, HI run02 52.480852s. Evidence11a-11o and query references
are indexed in findings.md. Tiny recorded USD/score differences meet the stated
tolerances; the community partitions matched in the compared runs.

The commands below are sectioned reproduction records, not instructions to
rerun completed trials. All named run IDs/logs/baseline files already exist.
Task12 SSM/failure/timeout/integrated acceptance is still pending. Current
Active labels may select run02; verify scope before manual analysis. No accepted
manual graph restoration or additional runner execution is required to mark
Task11 complete. EC2 uptime/cost and peak-memory reporting remain Task13 work.

## Existing resources

| Item | Value |
|---|---|
| Region | `us-east-1` |
| Instance | `is459-g2t1-ingest`, `i-0125056b848e5634a` |
| OS | Amazon Linux 2023, x86_64 |
| Instance size | Temporarily resized from `t3.small` to `t3.medium` with team agreement |
| Observed RAM / disk | About 3.7 GiB RAM after resize; 50 GiB root disk |
| Instance role | `is459-g2t1-ec2-s3-role` |
| Manually verified policies | `AmazonS3FullAccess`, `AmazonSSMManagedInstanceCore` |
| Login | AWS Systems Manager Session Manager, then `bash` |
| Docker | Client 25.0.14 / Engine 25.0.18 at setup |
| Image / observed database | `neo4j:5.26-community` / Neo4j 5.26.32 Community |
| GDS | `graph-data-science` plugin; verify live with `RETURN gds.version()` |
| Container | `role4-neo4j` |
| Persistent Docker volumes | `role4-data`, `role4-logs`, `role4-plugins` |
| Host work directory | `/home/ssm-user/role4/` |
| Local credentials | Protected `.env` in the host work directory; never copied into Git |

The attached S3/SSM policies do not establish Athena/Glue access for the new Python runner. Those permissions still need verification before an automated trial. The raw bucket stays read-only.

## First-time preparation, only if rebuilding the approved setup

The existing server is already prepared. On a fresh agreed Amazon Linux 2023 setup, check OS/RAM/disk and the attached role first, then prepare Docker and directories:

```bash
sudo dnf install -y docker python3
sudo systemctl enable --now docker
mkdir -p "$HOME/role4/import" "$HOME/role4/results"
sudo docker volume create role4-data
sudo docker volume create role4-logs
sudo docker volume create role4-plugins
```

AWS CLI v2 was already present on the observed server. Verify with `aws --version`; use the [official installation instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) if rebuilding without it. Do not configure personal AWS keys; use the instance role.

For a genuinely new database only, create the credential file interactively without printing the password. This preserves an existing file and does not reset the password of an existing database volume:

```bash
if [ ! -e "$HOME/role4/.env" ]; then
  (
    umask 077
    read -r -s -p 'Initial Neo4j password: ' role4_password
    printf '\n'
    if [ "${#role4_password}" -lt 8 ]; then
      printf 'Use a password of at least 8 characters.\n' >&2
      exit 1
    fi
    printf 'NEO4J_AUTH=neo4j/%s\n' "$role4_password" > "$HOME/role4/.env"
    unset role4_password
  )
else
  printf 'Existing credential file preserved.\n'
fi
```

Do not proceed to the container launch if preparation fails. Never capture the password or `.env` contents in screenshots, logs or video.

## Container configuration used

This is the launch configuration for reproduction or recovery, **not a command to run against an existing named container**. Initial preparation is recorded above. Never remove persistent volumes as part of a container-only recovery.

```bash
sudo docker run -d \
  --name role4-neo4j \
  --restart unless-stopped \
  --memory=2560m --memory-swap=2560m \
  -p 7474:7474 -p 7687:7687 \
  --env-file "$HOME/role4/.env" \
  -e 'NEO4J_PLUGINS=["graph-data-science"]' \
  -e 'NEO4J_dbms_security_procedures_unrestricted=gds.*' \
  -e NEO4J_server_memory_heap_initial__size=512M \
  -e NEO4J_server_memory_heap_max__size=1G \
  -e NEO4J_server_memory_pagecache_size=512M \
  -v role4-data:/data \
  -v role4-logs:/logs \
  -v role4-plugins:/plugins \
  -v "$HOME/role4/import:/var/lib/neo4j/import:rw" \
  neo4j:5.26-community
```

The import bind mount must be writable at container startup because this image changes its ownership. A read-only mount caused a restart loop during setup; that was corrected. File ownership can become UID/GID 7474, so use `sudo` for host downloads/header checks and verify readability as the container user.

HI files are under `import/hi-01/`; their Neo4j URLs are `file:///hi-01/bank_edges.csv` and `file:///hi-01/transactions.csv`. Sample files were kept separately. The database has scope-qualified Bank and Account uniqueness constraints; never mix a new CSV population into an accepted scope.

## Resume and stage input files

Use Session Manager on the running shared instance. Check `sudo docker ps -a`;
run `sudo docker start role4-neo4j` only if the existing container is Exited.
Do not recreate it. The following staging loop replaces repeated per-file steps.
It stages existing S3 objects; it does not import graph data.

```bash
graph_input_scope='li-01'  # select intended scope before running
graph_input_files=(bank_edges.csv transactions.csv)
# Bank-only cutoff comparison:
# graph_input_scope='hi-uncapped-01'
# graph_input_files=(bank_edges.csv)
sudo mkdir -p "$HOME/role4/import/$graph_input_scope"
sudo chmod 755 "$HOME/role4/import/$graph_input_scope"
for graph_input_file in "${graph_input_files[@]}"; do
  sudo aws s3 cp \
    "s3://is459-g2t1-aml-processed-bucket/bq1_graph_edges/$graph_input_scope/$graph_input_file" \
    "$HOME/role4/import/$graph_input_scope/$graph_input_file" --region us-east-1 || break
  sudo chmod 644 "$HOME/role4/import/$graph_input_scope/$graph_input_file" || break
  sudo docker exec --user 7474 role4-neo4j \
    head -n 1 "/var/lib/neo4j/import/$graph_input_scope/$graph_input_file" || break
  sudo wc -l "$HOME/role4/import/$graph_input_scope/$graph_input_file" || break
done
```

Run in Bash; stop on any error. Choose scope and file list before creating the
directory or starting the loop. Do not overwrite accepted inputs with a refreshed
source. Routine headers/counts do not need additional screenshots.

| Input | Bank CSV lines including header | Transaction CSV lines including header |
|---|---:|---:|
| Accepted HI | 29,375 | 34,892 |
| Accepted LI | 13,725 | 15,893 |
| Uncapped HI | 29,661 | Not exported: bank-only comparison |

Headers must match `BANK_HEADER`/`TX_HEADER` in `run_graph.py`. Counts/readability
are preliminary checks; reconcile actual graph counts/USD against Athena using
the selected import statements. All three populations were staged manually.

## Connection and verification

Security-group inbound TCP 7474 and 7687 are restricted to the user's current public IPv4 `/32` (console **My IP**), not the EC2 instance's IP. Preserve other team rules. Use Session Manager for terminal access. Browser connects to `http://EC2_PUBLIC_IP:7474` and Bolt on port 7687; the public IP can change after stopping/starting EC2.

In Session Manager:

```bash
bash
sudo docker ps -a
sudo docker logs --tail 50 role4-neo4j
```

In Neo4j Browser, run separately:

```cypher
CALL dbms.components();

RETURN gds.version() AS gds_version;

SHOW PROCEDURES YIELD name
WHERE name IN ['gds.graph.project','gds.betweenness.write','gds.louvain.write']
RETURN name;
```

The configured heap is 1 GiB and container cap 2.5 GiB; host free RAM is not all usable GDS heap. Inspect graph/algorithm estimates before running. The Python runner additionally limits algorithm estimates to a configurable budget but does not replace live capacity checks.

On 3 November 2026 the team approved Neo4j Browser instead of Bloom for the final visuals, as reported by the user. Use the existing Browser connection; no Bloom installation/licence is needed for the agreed route. Three final scenes are saved as 08p-08r and captioned in findings.md. Record the proposal-tool change in the report/slides.

## Automation trial preparation - readiness and release validation

Run in Session Manager/Bash on the existing host, one section at a time. These
checks passed manually on 6 November 2026 after the role policies were updated;
these preliminary checks alone do not establish completion. The subsequent direct trials passed; integrated execution remains pending.

```bash
aws athena get-work-group --work-group primary --region us-east-1 \
  --query 'WorkGroup.{State:State,EnforcedOutput:Configuration.EnforceWorkGroupConfiguration,Output:Configuration.ResultConfiguration.OutputLocation}' \
  --output json
aws glue get-table --database-name is459_g2t1_aml --name transactions_clean \
  --region us-east-1 --query 'Table.{Name:Name,Location:StorageDescriptor.Location}' \
  --output json
```

Workgroup was ENABLED, EnforcedOutput=false, with results under the Athena
bucket's `results/`. SELECT1 query `172a363c-c8cf-436e-9d0d-80251855c769`
succeeded and the instance read its CSV from S3. An equivalent minimal query is
shown below; capture the returned ID, check terminal state, then read its actual
result URI. Output-table creation and independent validation subsequently passed in the sample/HI trials.

```bash
aws athena start-query-execution --query-string 'SELECT 1 AS readiness_check' \
  --work-group primary --query-execution-context Database=is459_g2t1_aml \
  --result-configuration OutputLocation=s3://is459-g2t1-aml-athena-bucket/results/ \
  --region us-east-1 --query QueryExecutionId --output text
# Set the ID returned above, not a transaction ID:
role4_query_id='REPLACE_WITH_QUERY_ID'
aws athena get-query-execution --query-execution-id "$role4_query_id" \
  --region us-east-1 \
  --query 'QueryExecution.{State:Status.State,Output:ResultConfiguration.OutputLocation}' \
  --output json
# Only after SUCCEEDED: use the actual returned Output URI.
# aws s3 cp 's3://ACTUAL_OUTPUT_URI' - --region us-east-1 --only-show-errors
```

Release `role4-20261106-01` contains only `run_graph.py`, `export_edges.sql` and
`import.cypher`. It was staged in S3 and copied to a versioned host directory;
all three SHA256 hashes matched the offline-audited snapshot. No credentials,
data CSVs, clipboard helpers or personal runtime are part of this release.

```bash
role4_release='role4-20261106-01'
role4_code="/home/ssm-user/role4/code/$role4_release"
sudo mkdir -p "$role4_code"
for role4_source in run_graph.py export_edges.sql import.cypher; do
  sudo aws s3 cp \
    "s3://is459-g2t1-aml-athena-bucket/scripts/role4/$role4_release/$role4_source" \
    "$role4_code/$role4_source" --region us-east-1 || break
done
sudo sha256sum "$role4_code/run_graph.py" "$role4_code/export_edges.sql" \
  "$role4_code/import.cypher"
```

Do not overwrite an already accepted release with changed code; choose a new
release ID. The observed release hashes are:

| File | SHA256 |
|---|---|
| run_graph.py | 6d4765d61489e61832c668aee4c951518d66776b2eee42e76f4bd55f74757a9d |
| export_edges.sql | c4fa533d83290fd29f1354e70836ff6828e85a264bede632028cc061320322be |
| import.cypher | 02e489264d8e04b8d67fe421cced670a4e945aad2197634ab2f4377a48845d1d |

## Accepted-graph preservation check before automation

This authenticated read-only check uses the deployed runner's Neo4j client.
It verifies versions and captures a canonical fingerprint of saved HI node and
relationship properties. Active labels are intentionally excluded because the
runner switches them; stored graph data and saved scores must remain unchanged.
The original baseline file already exists: do not rerun capture with that name.
For a new future trial choose a distinct baseline filename before running.

```bash
sudo python3 - <<'PY'
import importlib.util
import hashlib
import json
from pathlib import Path

code = Path('/home/ssm-user/role4/code/role4-20261106-01/run_graph.py')
spec = importlib.util.spec_from_file_location('role4_check', code)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
db = runner.Neo4j(Path('/home/ssm-user/role4/.env'), 60)
print('Neo4j:', db.query('CALL dbms.components() YIELD versions,edition RETURN versions,edition'))
print('GDS:', db.query('RETURN gds.version() AS version'))
print('Active:', db.query('MATCH (b:ActiveBank) RETURN collect(DISTINCT b.scope) AS scopes,count(b) AS banks'))

def fingerprint(records):
    ordered = sorted(json.dumps(r, sort_keys=True) for r in records)
    return hashlib.sha256('\n'.join(ordered).encode()).hexdigest()

nodes = db.query("MATCH (n) WHERE n.scope='hi-01' RETURN properties(n) AS properties")
edges = db.query("MATCH (a {scope:'hi-01'})-[r]->(b {scope:'hi-01'}) "
                 'RETURN properties(a) AS source,type(r) AS relationship,'
                 'properties(r) AS properties,properties(b) AS target')
record = dict(scope='hi-01', nodes=len(nodes), relationships=len(edges),
              nodes_sha256=fingerprint(nodes), relationships_sha256=fingerprint(edges))
folder = Path('/home/ssm-user/role4/trial-evidence')
folder.mkdir(exist_ok=True)
path = folder / 'hi-baseline-before-sample-auto-20261106-01.json'
with path.open('x') as handle:
    json.dump(record, handle, indent=2)
print(json.dumps(record, indent=2))
print('Saved:', path)
PY
```

Reported baseline:46,615 scoped nodes and92,612 relationships. Node SHA256:
`8d3989fd83973bf052462ab1241fc60ce78ac355bb047ce291f614b0476e4fc3`;
relationship SHA256:
`f1202c85c504a1ea24f9d5b083b0eaeab60181e06c419c0f2ef20bd5feceba56`.
After a trial, recompute using the same queries/canonicalisation and compare
with the saved baseline without overwriting it. Post-trial verification subsequently passed after sample, HI run01 and HI run02. These hashes contain no credentials; never print `.env` contents.

## Direct sample automation trial - validated

Executed 6 November2026: sample-auto-20261106-01 returned SUCCEEDED in37.155682s,
runner exit0/log exit0. Stage times are recorded in findings.md. Do not rerun the
same ID/log. Published outputs, accepted-HI preservation and independent feature coverage passed. Subsequent full-HI/repeat trials passed; SSM/integrated trials remain pending.

Prerequisites: the verified release above, saved HI baseline, ready Neo4j/GDS,
and no concurrent manual graph analysis or ETL source refresh. The trial uses a
fresh scope and changes Active labels/projections; accepted scoped properties
must remain unchanged. This is the first automated sample trial, not a repeat
of the manual sample workflow. It also tests output-table creation permissions.

The executed command is retained below. Existing logs are preserved; choose a new run
ID/log for a later trial instead of overwriting evidence or retrying partial
prefixes. Do not claim success from a SendCommand acknowledgement or a single
completed stage. Require the runner's final SUCCEEDED record and exit code0,
then independently inspect its receipt/outputs and the accepted HI fingerprint.

```bash
set -o pipefail
role4_trial_log='/home/ssm-user/role4/trial-evidence/sample-auto-20261106-01.log'
if sudo test -e "$role4_trial_log"; then
  printf 'Trial log already exists. Stop and inspect the previous run.\n'
else
  sudo python3 -u /home/ssm-user/role4/code/role4-20261106-01/run_graph.py \
    --run-id sample-auto-20261106-01 \
    --dataset sample \
    2>&1 | sudo tee "$role4_trial_log"
  role4_trial_status=("${PIPESTATUS[@]}")
  printf 'Runner exit code: %s\nLog exit code: %s\n' \
    "${role4_trial_status[0]}" "${role4_trial_status[1]}" \
    | sudo tee -a "$role4_trial_log"
fi
```

The log and eventual receipt record timings. This does not measure total EC2
billed uptime or observed peak RAM; collect those separately if available.
Do not close the terminal or stop EC2 while the trial is running. Capture the
meaningful final status and run/table identity after inspection, not every
intermediate timing line. Failures must be investigated before another run.

## Post-trial accepted-HI fingerprint verification

The published sample receipt was read from S3:42 banks,35 corridors/transfers,
USD434634.33;8 communities,modularity0.7102040816326531,42 assignments. These
match the earlier manual sample totals. The preservation command below executed
and returned all five checks true and unchanged=true, with46,615 nodes and92,612
relationships. It never writes graph data or overwrites the original baseline.
Its verification file now exists; do not rerun with the same output filename.

```bash
sudo python3 - <<'PY'
import importlib.util
import hashlib
import json
from pathlib import Path

home = Path('/home/ssm-user/role4')
spec = importlib.util.spec_from_file_location(
    'role4_check', home / 'code/role4-20261106-01/run_graph.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
db = runner.Neo4j(home / '.env', 60)
baseline = json.loads((home / 'trial-evidence/hi-baseline-before-sample-auto-20261106-01.json').read_text())

def fingerprint(records):
    ordered = sorted(json.dumps(r, sort_keys=True) for r in records)
    return hashlib.sha256('\n'.join(ordered).encode()).hexdigest()

nodes = db.query("MATCH (n) WHERE n.scope='hi-01' RETURN properties(n) AS properties")
edges = db.query("MATCH (a {scope:'hi-01'})-[r]->(b {scope:'hi-01'}) "
                 'RETURN properties(a) AS source,type(r) AS relationship,'
                 'properties(r) AS properties,properties(b) AS target')
current = dict(scope='hi-01', nodes=len(nodes), relationships=len(edges),
               nodes_sha256=fingerprint(nodes), relationships_sha256=fingerprint(edges))
checks = {key: current[key] == baseline[key] for key in current}
result = dict(checks=checks, unchanged=all(checks.values()), current=current)
output = home / 'trial-evidence/hi-preservation-after-sample-auto-20261106-01.json'
with output.open('x') as handle:
    json.dump(result, handle, indent=2)
print(json.dumps(result, indent=2))
print('Saved:', output)
if not result['unchanged']:
    raise SystemExit('HI preservation check failed: investigate before another run')
PY
```

Expected:all checks true,46615 nodes and92612 relationships. A mismatch must be
investigated rather than overwriting the baseline or claiming preservation.

## Direct full-HI automation trial - validated

Executed6 November2026: hi-auto-20261106-01 returned SUCCEEDED in63.522618s,
runner exit0/log exit0. Detailed stage times are in findings.md. Do not rerun
this ID/log. Published output, manual-baseline score/partition comparison and accepted-HI fingerprint preservation passed. The following is the command record.

The first direct sample trial passed terminal success, S3 receipt inspection,
accepted-HI fingerprint preservation and an independent Athena feature-table
check (42,42,8,0). Evidence11a-11d is saved; the independent Athena query ID is
still to be recorded. Do not repeat the sample run merely to recover that ID.

Next run: `hi-auto-20261106-01`, dataset HI-Medium, same audited release. Keep
manual graph work and ETL source refreshes idle during the run. Existing hi-01
is not its scope or output destination. Its saved fingerprint remains the
preservation baseline. This does not implement or rerun the uncapped experiment.

```bash
set -o pipefail
role4_trial_log='/home/ssm-user/role4/trial-evidence/hi-auto-20261106-01.log'
if sudo test -e "$role4_trial_log"; then
  printf 'Trial log already exists. Stop and inspect the previous run.\n'
else
  sudo python3 -u /home/ssm-user/role4/code/role4-20261106-01/run_graph.py \
    --run-id hi-auto-20261106-01 \
    --dataset HI-Medium \
    2>&1 | sudo tee "$role4_trial_log"
  role4_trial_status=("${PIPESTATUS[@]}")
  printf 'Runner exit code: %s\nLog exit code: %s\n' \
    "${role4_trial_status[0]}" "${role4_trial_status[1]}" \
    | sudo tee -a "$role4_trial_log"
fi
```

Do not reuse an existing/partial run ID. Keep EC2/terminal running until the
command ends. Independent output validation, manual-baseline comparison and
accepted-HI fingerprint verification are required afterward; terminal success
alone does not close those checks. Compare community memberships/coverage
appropriately rather than assuming identical numeric community IDs on reruns.

## Fresh-ID repeatability trial - prepare first automated HI baseline

Sample and first full-HI trials passed terminal/receipt/coverage checks. Manual
HI fingerprint preservation passed after both. Manual/automated HI bank IDs,
scores (within stated tolerance) and community partition also agree. Next is
a second HI execution with a different ID, not a retry of the first prefix.

Before that run, capture the first automated scope with the same canonical
fingerprint method. The original manual HI baseline is retained separately.
The following is read-only graph access; baseline capture is not a GDS rerun.
Capture completed:46615 nodes,92612 relationships. Nodes SHA256:
`7c79e2160035afc56daef339286c002163d55de6e7772cdcfb031e3be222f9a6`;
relationships SHA256:
`a3eb33d05f005c4688488b039d79e6419a0a1546c1a8c31836da18fe71abd2cf`.
Its baseline file now exists; do not rerun capture with that filename. The second HI execution subsequently passed, including preservation and repeat comparisons below.

```bash
sudo python3 - <<'PY'
import importlib.util
import hashlib
import json
from pathlib import Path

home = Path('/home/ssm-user/role4')
scope = 'hi-auto-20261106-01'
spec = importlib.util.spec_from_file_location(
    'role4_check', home / 'code/role4-20261106-01/run_graph.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
db = runner.Neo4j(home / '.env', 60)

def fingerprint(records):
    ordered = sorted(json.dumps(r, sort_keys=True) for r in records)
    return hashlib.sha256('\n'.join(ordered).encode()).hexdigest()

nodes = db.query('MATCH (n {scope:$scope}) RETURN properties(n) AS properties',
                 {'scope': scope})
edges = db.query('MATCH (a {scope:$scope})-[r]->(b {scope:$scope}) '
                 'RETURN properties(a) AS source,type(r) AS relationship,'
                 'properties(r) AS properties,properties(b) AS target',
                 {'scope': scope})
if not nodes or not edges:
    raise SystemExit('Expected first automated HI graph is missing')
record = dict(scope=scope,nodes=len(nodes),relationships=len(edges),
              nodes_sha256=fingerprint(nodes),relationships_sha256=fingerprint(edges))
path = home / 'trial-evidence/hi-auto-01-before-repeat.json'
with path.open('x') as handle:
    json.dump(record,handle,indent=2)
print(json.dumps(record,indent=2))
print('Saved:',path)
PY
```

Expected population sizes:46615 scoped nodes and92612 relationships. Hashes
belong to the automated scope and need not equal the manual-scope hashes.
Do not overwrite either baseline. After a second run, verify both earlier
scopes plus output-prefix separation and new-run completeness before closing
the repeatability criterion. Graph Active labels are intentionally not hashed.

## Fresh-ID second HI execution - validated

Run02 returned SUCCEEDED in52.480852s with runner exit0/log exit0. Stage times
are recorded in findings.md. Do not rerun this ID/log. Output/source comparisons and preservation of both earlier graphs subsequently passed.

The reviewed release is unchanged. Keep other graph analysis and ETL refreshes
idle. A fresh run intentionally creates its own scope-qualified graph records;
this is different from rerunning MERGE within an existing accepted scope. Do not
overwrite first-run prefixes or receipts. Collect repeat runtime without a
separate measurement-only run.

```bash
set -o pipefail
role4_trial_log='/home/ssm-user/role4/trial-evidence/hi-auto-20261106-02.log'
if sudo test -e "$role4_trial_log"; then
  printf 'Trial log already exists. Stop and inspect the previous run.\n'
else
  sudo python3 -u /home/ssm-user/role4/code/role4-20261106-01/run_graph.py \
    --run-id hi-auto-20261106-02 \
    --dataset HI-Medium \
    2>&1 | sudo tee "$role4_trial_log"
  role4_trial_status=("${PIPESTATUS[@]}")
  printf 'Runner exit code: %s\nLog exit code: %s\n' \
    "${role4_trial_status[0]}" "${role4_trial_status[1]}" \
    | sudo tee -a "$role4_trial_log"
fi
```

After terminal success, inspect the new receipt/input manifest and compare
source hashes/totals, unique feature coverage and output identity with run01.
Recheck both accepted manual HI and first automated HI fingerprints. Do not
claim repeatability from terminal success alone.

## Verify both earlier HI scopes after run02 - passed

User-reported result:all_preserved=true;all five checks true for manual hi-01 and
first automated hi-auto-20261106-01. Report saved as shown below;do not overwrite
it by rerunning the same capture. Evidence11k is saved. Independent run02 feature
validation also passed5159,5159,113,0 (queryd55dc6f8-0a52-467e-9bc0-dc286429be15).

Run02's S3 receipt matches run01's reported population totals and113 community
assignments across5159 banks. Next compare both earlier graphs with their saved
pre-trial fingerprints. This reads stored properties without rerunning algorithms
or changing Active labels. The report is a new local file; baselines are retained.

```bash
sudo python3 - <<'PY'
import importlib.util
import hashlib
import json
from pathlib import Path

home = Path('/home/ssm-user/role4')
spec = importlib.util.spec_from_file_location(
    'role4_check', home / 'code/role4-20261106-01/run_graph.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
db = runner.Neo4j(home / '.env', 60)

def fingerprint(records):
    ordered = sorted(json.dumps(r, sort_keys=True) for r in records)
    return hashlib.sha256('\n'.join(ordered).encode()).hexdigest()

baselines = {
    'hi-01': 'hi-baseline-before-sample-auto-20261106-01.json',
    'hi-auto-20261106-01': 'hi-auto-01-before-repeat.json'
}
results = {}
for scope, filename in baselines.items():
    baseline = json.loads((home / 'trial-evidence' / filename).read_text())
    nodes = db.query('MATCH (n {scope:$scope}) RETURN properties(n) AS properties',
                     {'scope': scope})
    edges = db.query('MATCH (a {scope:$scope})-[r]->(b {scope:$scope}) '
                     'RETURN properties(a) AS source,type(r) AS relationship,'
                     'properties(r) AS properties,properties(b) AS target',
                     {'scope': scope})
    current = dict(scope=scope,nodes=len(nodes),relationships=len(edges),
                   nodes_sha256=fingerprint(nodes),relationships_sha256=fingerprint(edges))
    checks = {key: current[key] == baseline[key] for key in current}
    results[scope] = dict(unchanged=all(checks.values()),checks=checks)

report = dict(all_preserved=all(r['unchanged'] for r in results.values()),scopes=results)
path = home / 'trial-evidence/hi-preservation-after-repeat-02.json'
with path.open('x') as handle:
    json.dump(report,handle,indent=2)
print(json.dumps(report,indent=2))
print('Saved:',path)
if not report['all_preserved']:
    raise SystemExit('Preservation failed: investigate before another run')
PY
```

Expected:all_preserved=true and all checks true for both scopes. Do not overwrite
the report if it already exists. A mismatch requires investigation before
claiming accepted-results preservation or starting SSM/integration trials.

## Compare repeat-run lineage and input hashes - checked; byte difference investigated

Both run01 and run02 have independently validated feature coverage5159,5159,
113,0, and earlier manual/automated HI graphs passed preservation. Compare their
existing S3 receipts without a new source scan. Source CSV hash equality proves
byte-identical recorded exports; differing bank CSV hashes require semantic
comparison because aggregate floating-point representations can differ. Do not
treat equal counts alone as proof of identical source contents.

```bash
sudo python3 - <<'PY'
import json
import subprocess
from pathlib import Path

home = Path('/home/ssm-user/role4')
run_ids = ['hi-auto-20261106-01', 'hi-auto-20261106-02']
prefix = 's3://is459-g2t1-aml-processed-bucket/bq1_graph_results/'
receipts = []
for run_id in run_ids:
    raw = subprocess.check_output([
        'aws','s3','cp',prefix+run_id+'/success.json','-',
        '--region','us-east-1','--only-show-errors'])
    receipts.append(json.loads(raw))
first, second = receipts
local_first = json.loads((home/'pipeline-runs'/run_ids[0]/'success.json').read_text())
checks = {
    'correct_run_ids': [r['run_id'] for r in receipts] == run_ids,
    'both_succeeded': all(r['status']=='SUCCEEDED' for r in receipts),
    'same_dataset_and_filter': (first['dataset'],first['filter']) == (second['dataset'],second['filter']),
    'same_code_hashes': first['code_sha256']==second['code_sha256'],
    'same_totals': first['totals']==second['totals'],
    'separate_feature_tables': first['feature_table']!=second['feature_table'],
    'correct_separate_output_paths': all(r['feature_location']==prefix+run_id+'/bank_features/' for r,run_id in zip(receipts,run_ids)),
    'first_published_receipt_unchanged': first==local_first
}
input_hashes = {name:first['files'][name]['sha256']==second['files'][name]['sha256']
                for name in ('bank_edges','transactions')}
report = dict(checks=checks,input_hashes_match=input_hashes)
path = home/'trial-evidence/hi-repeat-input-comparison.json'
with path.open('x') as handle:
    json.dump(report,handle,indent=2)
print(json.dumps(report,indent=2))
print('Saved:',path)
PY
```

Investigate any false check. This verifies recorded input lineage and receipt
preservation; it does not replace numerical feature/partition comparison or
claim that every S3 object has been independently rehashed.

## Investigate differing repeat bank CSV hashes - within tolerance

Executed result:both staged-file hashes match receipts;29374 unique pairs per
run,0 missing/extra pairs,matching order,0 count differences.140 USD values differ
by at most1E-9 dollars;none exceeds one cent. This is consistent with tiny
floating-point aggregate variation,not a proven byte-identical bank export.
The comparison report now exists;do not rerun with the same filename.

All8 repeat lineage checks passed;transaction CSV hashes match,bank CSV hashes
differ. Compare actual staged CSVs with each receipt's recorded hash,then compare
unique bank-pair keys,integer counts and Decimal USD totals. Report any numerical
difference rather than assuming it is formatting. Tolerance is one cent per pair,
consistent with the runner's primary per-pair input reconciliation.

```bash
sudo python3 - <<'PY'
import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path

home = Path('/home/ssm-user/role4')
run_ids = ['hi-auto-20261106-01','hi-auto-20261106-02']
tables, orders, verified = [], [], {}
for run_id in run_ids:
    receipt = json.loads((home/'pipeline-runs'/run_id/'success.json').read_text())
    path = home/'import'/('auto-'+run_id)/'bank_edges.csv'
    verified[run_id] = hashlib.sha256(path.read_bytes()).hexdigest() == receipt['files']['bank_edges']['sha256']
    with path.open(encoding='utf-8-sig',newline='') as handle:
        rows = list(csv.DictReader(handle))
    keys = [(r['from_bank'],r['to_bank']) for r in rows]
    if len(set(keys)) != len(rows):
        raise SystemExit('Duplicate bank pair: '+run_id)
    tables.append(dict(zip(keys,rows)))
    orders.append(keys)
first, second = tables
common = first.keys() & second.keys()
deltas = [abs(Decimal(first[k]['laundering_usd'])-Decimal(second[k]['laundering_usd'])) for k in common]
report = {
    'staged_files_match_receipts':verified,
    'run01_pairs':len(first),'run02_pairs':len(second),
    'run01_only_pairs':len(first.keys()-second.keys()),
    'run02_only_pairs':len(second.keys()-first.keys()),
    'pair_order_matches':orders[0]==orders[1],
    'transaction_count_mismatches':sum(int(first[k]['laundering_txns'])!=int(second[k]['laundering_txns']) for k in common),
    'usd_text_differences':sum(first[k]['laundering_usd']!=second[k]['laundering_usd'] for k in common),
    'usd_exact_numeric_differences':sum(d!=0 for d in deltas),
    'usd_differences_over_one_cent':sum(d>Decimal('0.01') for d in deltas),
    'max_absolute_usd_delta':str(max(deltas,default=Decimal(0)))
}
path = home/'trial-evidence/hi-repeat-bank-input-semantic-comparison.json'
with path.open('x') as handle:
    json.dump(report,handle,indent=2)
print(json.dumps(report,indent=2))
print('Saved:',path)
PY
```

This reads existing host inputs and receipts;no new Athena scan/import/GDS run.
Stop on a receipt-hash mismatch or changed pair keys/counts. Investigate USD
differences beyond the stated tolerance. The report preserves small differences
even when they meet tolerance;row ordering is not a semantic bank-pair difference.

## Task 12 isolated SSM execution-timeout probe (passed 7 November 2026)

Observed Command ID `0c6bc427-bcd0-4cbf-b618-69a75ca1d8bf`: status Timed Out,
detailed status Execution Timed Out,response137,start marker only. The displayed
interval was30 seconds. Evidence:
[12e SSM timeout](screenshots/12e-ssm-execution-timeout.png).

Use the AWS-owned `AWS-RunShellScript` Command document on the existing instance
in us-east-1. This is an isolated SSM timeout probe, not the graph runner or an
integrated Step Functions timeout test. Leave the Role 4 document unchanged.

Set the document's `executionTimeout` parameter to `30` seconds; leave its
working directory empty. Use these commands:

```sh
printf '%s\n' 'ROLE4_TIMEOUT_PROBE_STARTED'
sleep 90
printf '%s\n' 'UNEXPECTED_TIMEOUT_PROBE_COMPLETED'
```

Send output to `s3://is459-g2t1-aml-athena-bucket/role4-ssm-logs/timeout-probe-20261107-01/`.
The execution-timeout parameter is distinct from the command delivery timeout
under additional settings; retain the delivery-timeout default. The script
does not call Neo4j/Athena or write graph outputs. Expect invocation detail
`Execution timed out` (API `ExecutionTimedOut`) and no completion marker. Record
the actual Command ID,status,response code and output rather than assuming
an exact response code. Save meaningful terminal evidence only after checking.
This probe alone does not establish cancellation of database activity or
failure propagation through the shared workflow; those require separate tests.

## Evidence and stopping

- [Running container](screenshots/02k-neo4j-container-started.png)
- [Database version](screenshots/02m-neo4j-version.png)
- [GDS version](screenshots/02n-gds-version.png)
- [Required GDS procedures](screenshots/02o-gds-required-procedures.png)

Save accepted results first. Coordinate with the group, end the Session Manager session and stop the EC2 instance when idle; do not terminate it during development. Stopping a browser tab or Docker alone does not stop EC2 charges. Final resource deletion is coordinated after the presentation.
