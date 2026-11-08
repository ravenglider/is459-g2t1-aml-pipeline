# CloudShell commands: HI acceptance first, then LI

**PREPARED, NOT EXECUTED.** No AWS writes or paid Athena queries have been run
by this workstream. The commands below must not be pasted as one unattended
script. Review and run each stage only after its gate is satisfied. Athena
SELECTs incur charges and write query results to the approved result bucket;
they do not write sequence datasets or Catalog tables.

## Gates and order

| Stage | Before publication? | Required condition |
|---|---|---|
| Local SQL preparation and checklist | Yes; no AWS required | Reviewed validation branch |
| Metadata/source-snapshot collection | Yes | Coordinated AWS read access, actual job references |
| Source-count/quality SQL | Yes | Explicit paid-query approval and frozen job source |
| Staged generation validation/copy/read-back | Generation owner's responsibility | Not executed by these commands |
| Production schema, date coverage, counts, IDs, labels, paths and lineage | No | Full selected dataset PUBLISHED, stable Catalog, approved read window |
| Shared 55-date check and final HI/LI handoff | After both publications | HI and LI independently accepted |

Before any publication the generation owner must have explicit confirmations
from **both Sonia and Xiang Ying**. See [the messages and confirmation table](READER_PAUSE_AND_HANDOFF.md).
Those confirmations remain PENDING here. A quiet chat or a completed profiler
does not authorise publication. Paid-query approval is a separate gate.
The integrated `glue_full_generation.py` job generates and publishes in one
invocation. Obtain both pause confirmations before the generation owner starts
that publication-enabled job; do not assume there is a manual pause between
generation and the Catalog switch. Read-only source checks can precede it.

## 1. Prepare individual SQL files locally in CloudShell

Use a fresh local directory. No generation/profiler script is executed.

```bash
set -euo pipefail
ROLE5_SESSION="$(date -u +%Y%m%dT%H%M%SZ)"
ROLE5_WORKSPACE="$HOME/role5-validation-$ROLE5_SESSION"
git clone --single-branch --branch role5/full-validation \
  https://github.com/ravenglider/is459-g2t1-aml-pipeline.git "$ROLE5_WORKSPACE"
cd "$ROLE5_WORKSPACE"
ROLE5_COMMIT="$(git rev-parse HEAD)"
ROLE5_LOCAL="$HOME/role5-acceptance-$ROLE5_SESSION"
mkdir -p "$ROLE5_LOCAL/evidence"
ROLE5_QUERIES="$ROLE5_LOCAL/queries"
python3 bq2/sequences/validation/prepare_queries.py --output "$ROLE5_QUERIES"
printf '%s\n' "$ROLE5_COMMIT" > "$ROLE5_LOCAL/evidence/validation-commit.txt"
```

Expect **75 SQL files** plus `query-plan.json`: one statement per file, with
phases and SHA-256 hashes. Both the full lineage query and the alternative
date chunks are prepared; choose one complete coverage strategy, not both.
Do not modify generated files silently; rerender from reviewed source if needed.

## 2. Configure identity and collect read-only metadata

Fill the verified account and actual references first. This block does not grant
permission; run it only after coordinated read access. Use no permanent keys.

```bash
ROLE5_REGION='us-east-1'
ROLE5_DATABASE='is459_g2t1_aml'
ROLE5_WORKGROUP='primary'
ROLE5_EXPECTED_ACCOUNT='<verified 12-digit group account>'
ROLE5_READ_APPROVAL='<actual AWS metadata-read approval reference>'
[[ "$ROLE5_EXPECTED_ACCOUNT" =~ ^[0-9]{12}$ ]] || { echo 'Fill verified account'; false; }
[[ -n "$ROLE5_READ_APPROVAL" && "$ROLE5_READ_APPROVAL" != '<'* ]] || { echo 'Read approval missing'; false; }
ROLE5_ACTUAL_ACCOUNT="$(aws sts get-caller-identity --region "$ROLE5_REGION" --query Account --output text)"
[[ "$ROLE5_ACTUAL_ACCOUNT" = "$ROLE5_EXPECTED_ACCOUNT" ]] || { echo 'Wrong AWS account'; false; }
aws athena get-work-group --work-group "$ROLE5_WORKGROUP" --region "$ROLE5_REGION" \
  --output json > "$ROLE5_LOCAL/evidence/workgroup.json"
python3 - "$ROLE5_LOCAL/evidence/workgroup.json" <<'PY'
import json,sys
w=json.load(open(sys.argv[1]))['WorkGroup']
assert w['State']=='ENABLED', 'Workgroup disabled'
assert w['Configuration']['EngineVersion']['EffectiveEngineVersion']=='Athena engine version 3', 'Engine 3 required'
print('Review workgroup result location, encryption and scan limits before paid queries.')
PY
aws glue get-table --database-name "$ROLE5_DATABASE" --name transactions_clean \
  --region "$ROLE5_REGION" --output json > "$ROLE5_LOCAL/evidence/source-table.json"
aws glue get-partitions --database-name "$ROLE5_DATABASE" --table-name transactions_clean \
  --region "$ROLE5_REGION" --output json > "$ROLE5_LOCAL/evidence/source-partitions.json"
```

AWS CLI pagination stays enabled: do not add `--no-paginate` or a truncating
`--max-items` to Catalog exports. Compare source metadata to the generation
owner's exact object/table/partition fingerprints. GetPartitions alone cannot
recreate the complete source-object inventory hash. Obtain that inventory and
the job-bound fingerprints from the owner; do not execute their fingerprint or
profiler code. Metadata matches are change evidence, not a transactional lock.

## 3. Prepare the paid-query runner, without running a query

Fill these only from explicit coordination. Review the workgroup's actual scan
limit and approved cost/time budget. Result locations may be overridden by the
workgroup; the helper records the location returned by Athena.

```bash
ROLE5_QUERY_APPROVAL_REF='<actual approval for paid read-only checks>'
ROLE5_BUDGET_REF='<agreed scan budget and query timeout reference>'
ROLE5_SOURCE_FREEZE_REF='<job-bound source snapshot and no-refresh agreement>'
ROLE5_STABLE_OUTPUT_REF=''  # Fill only after a verified publication and stable read window.
ROLE5_RESULTS="s3://is459-g2t1-aml-athena-bucket/results/role5-acceptance/$ROLE5_SESSION/"
ROLE5_MAX_POLLS=360  # Review: 360 polls at 5s = about 30 minutes; not an AWS cancellation deadline.
ROLE5_POLL_SECONDS=5

role5_query() {
  local r5_file="$1" r5_label="$2" r5_phase r5_request r5_id r5_status r5_state r5_location r5_poll
  [[ "$r5_label" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || return 2
  [[ -n "$ROLE5_QUERY_APPROVAL_REF" && "$ROLE5_QUERY_APPROVAL_REF" != '<'* ]] || { echo 'Paid-query approval missing'; return 2; }
  [[ -n "$ROLE5_BUDGET_REF" && "$ROLE5_BUDGET_REF" != '<'* ]] || { echo 'Budget approval missing'; return 2; }
  [[ -n "$ROLE5_SOURCE_FREEZE_REF" && "$ROLE5_SOURCE_FREEZE_REF" != '<'* ]] || { echo 'Source freeze missing'; return 2; }
  [[ "$ROLE5_MAX_POLLS" =~ ^[1-9][0-9]*$ && "$ROLE5_POLL_SECONDS" =~ ^[1-9][0-9]*$ ]] || return 2
  r5_phase="$(python3 - "$ROLE5_QUERIES/query-plan.json" "$r5_file" <<'PY'
import hashlib,json,pathlib,sys
plan=pathlib.Path(sys.argv[1]); f=pathlib.Path(sys.argv[2]).resolve()
items=[q for q in json.loads(plan.read_text())['queries'] if (plan.parent/q['path']).resolve()==f]
assert len(items)==1, 'File not in reviewed plan'
assert hashlib.sha256(f.read_bytes()).hexdigest()==items[0]['sha256'], 'SQL changed after preparation'
print(items[0]['phase'])
PY
  )" || return 2
  if [[ "$r5_phase" != source ]]; then
    [[ -n "$ROLE5_STABLE_OUTPUT_REF" && "$ROLE5_STABLE_OUTPUT_REF" != '<'* ]] || { echo 'Verified stable publication/read window missing'; return 2; }
  fi
  [[ "$(aws sts get-caller-identity --region "$ROLE5_REGION" --query Account --output text)" = "$ROLE5_EXPECTED_ACCOUNT" ]] || { echo 'Wrong AWS account'; return 2; }
  r5_request="$ROLE5_LOCAL/evidence/$r5_label.request.json"
  [[ ! -e "$r5_request" ]] || { echo 'Existing request: inspect/resume its query ID; do not resubmit'; return 2; }
  python3 - "$r5_file" "$r5_request" "$ROLE5_DATABASE" "$ROLE5_WORKGROUP" "$ROLE5_RESULTS" <<'PY'
import json,pathlib,sys,uuid
with open(sys.argv[2],'x') as f:
    json.dump(dict(QueryString=pathlib.Path(sys.argv[1]).read_text(),ClientRequestToken=uuid.uuid4().hex,
        QueryExecutionContext=dict(Database=sys.argv[3],Catalog='AwsDataCatalog'),
        WorkGroup=sys.argv[4],ResultConfiguration=dict(OutputLocation=sys.argv[5])),f)
PY
  [[ $? = 0 ]] || return 2
  r5_id="$(aws athena start-query-execution --region "$ROLE5_REGION" \
    --cli-input-json "file://$r5_request" --query QueryExecutionId --output text)" || return 1
  printf '%s\n' "$r5_id" > "$ROLE5_LOCAL/evidence/$r5_label.query-id.txt"
  r5_status="$ROLE5_LOCAL/evidence/$r5_label.execution.json"
  for ((r5_poll=1; r5_poll<=ROLE5_MAX_POLLS; r5_poll++)); do
    aws athena get-query-execution --query-execution-id "$r5_id" --region "$ROLE5_REGION" \
      --output json > "$r5_status" || return 1
    r5_state="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["QueryExecution"]["Status"]["State"])' "$r5_status")" || return 1
    case "$r5_state" in
      SUCCEEDED) break ;;
      FAILED|CANCELLED) cat "$r5_status"; return 1 ;;
      QUEUED|RUNNING) sleep "$ROLE5_POLL_SECONDS" ;;
      *) echo 'Unexpected query state'; return 1 ;;
    esac
  done
  if [[ "$r5_state" != SUCCEEDED ]]; then
    echo "Polling limit reached: $r5_id may still run. Coordinate cancellation; do not resubmit."
    return 124
  fi
  r5_location="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["QueryExecution"]["ResultConfiguration"]["OutputLocation"])' "$r5_status")" || return 1
  aws s3 cp "$r5_location" "$ROLE5_LOCAL/evidence/$r5_label.results.csv" --region "$ROLE5_REGION" || return 1
  printf 'QUERY SUCCEEDED: %s; inspect results before accepting or running the next check.\n' "$r5_id"
}
```

The polling limit does not cancel a server-side query. On timeout/failure,
stop, retain the query ID and coordinate with the owner; do not start duplicate
queries. If submission outcome is uncertain, inspect Athena history/the saved
request token before any retry. The helper does not auto-retry paid submissions.
Inspect `execution.json` for actual output location, effective engine, runtime
and scanned bytes. SQL success is not acceptance: inspect the returned counters.

## 4. HI first: source checks (may precede publication)

After approvals/source freeze, run each line separately and inspect its result:

```bash
role5_query "$ROLE5_QUERIES/HI-Medium/02-source-summary.sql" hi-source-summary
role5_query "$ROLE5_QUERIES/HI-Medium/03-source-daily.sql" hi-source-daily
```

Expected HI source: **31,898,218 transactions, 35,230 positive labels, 28 dates**,
September 1–28. Invalid/duplicate/null counters must be zero; reconcile daily
counts to the final job source. These are not sequence counts.

## 5. HI post-publication metadata and local receipt checks

Do not proceed for staged-only, partial or RECOVERY_REQUIRED output. First
obtain both reader-pause confirmations and the owner's verified publication.
During an authorised stable read window fill the actual application identities:

```bash
ROLE5_DATASET='HI-Medium'
ROLE5_RUN_ID='<actual HI application run ID>'
ROLE5_ATTEMPT_ID='<actual HI application attempt ID>'
ROLE5_GLUE_RUN_ID='<actual HI Glue jr_... ID>'
ROLE5_STABLE_OUTPUT_REF='<verified HI publication and coordinated read-window reference>'
ROLE5_DATA_EVIDENCE="$ROLE5_LOCAL/evidence/$ROLE5_DATASET"
mkdir -p "$ROLE5_DATA_EVIDENCE"
aws glue get-job --job-name is459-g2t1-bq2-sequence-builder --region "$ROLE5_REGION" \
  --output json > "$ROLE5_DATA_EVIDENCE/job-definition-private.json"
aws glue get-job-run --job-name is459-g2t1-bq2-sequence-builder --run-id "$ROLE5_GLUE_RUN_ID" \
  --region "$ROLE5_REGION" --output json > "$ROLE5_DATA_EVIDENCE/job-run-private.json"
aws glue get-table --database-name "$ROLE5_DATABASE" --name bq2_sequences \
  --region "$ROLE5_REGION" --output json > "$ROLE5_DATA_EVIDENCE/table.json"
aws glue get-partitions --database-name "$ROLE5_DATABASE" --table-name bq2_sequences \
  --region "$ROLE5_REGION" --output json > "$ROLE5_DATA_EVIDENCE/partitions-response.json"
python3 - "$ROLE5_DATA_EVIDENCE/partitions-response.json" "$ROLE5_DATA_EVIDENCE/partitions.json" <<'PY'
import json,sys
with open(sys.argv[2],'x') as f: json.dump(json.load(open(sys.argv[1]))['Partitions'],f,indent=2)
PY
ROLE5_MANIFEST="s3://is459-g2t1-aml-processed-bucket/bq2_sequences_staging/full/dataset=$ROLE5_DATASET/$ROLE5_RUN_ID/$ROLE5_ATTEMPT_ID/metadata/run.json"
ROLE5_PUBLICATION="s3://is459-g2t1-aml-processed-bucket/bq2_sequences_metadata/production/dataset=$ROLE5_DATASET/$ROLE5_RUN_ID/$ROLE5_ATTEMPT_ID/publication.json"
aws s3 cp "$ROLE5_MANIFEST" "$ROLE5_DATA_EVIDENCE/run.json" --region "$ROLE5_REGION"
aws s3 cp "$ROLE5_PUBLICATION" "$ROLE5_DATA_EVIDENCE/publication.json" --region "$ROLE5_REGION"
python3 bq2/sequences/validation/review_run.py \
  --manifest "$ROLE5_DATA_EVIDENCE/run.json" \
  --publication "$ROLE5_DATA_EVIDENCE/publication.json" \
  --partitions "$ROLE5_DATA_EVIDENCE/partitions.json"
```

Confirm these exact URIs with the owner; do not guess missing IDs. Require Glue
SUCCEEDED, matching full report/receipts and source/release evidence. Local
`EVIDENCE_BINDING_PASSED` is not live Athena acceptance. Keep raw job arguments
and approvals private; redact secrets before sharing any summary.

## 6. HI post-publication paid checks, one at a time

```bash
role5_query "$ROLE5_QUERIES/00-schema.sql" hi-schema
role5_query "$ROLE5_QUERIES/HI-Medium/01-partitions.sql" hi-partitions
role5_query "$ROLE5_QUERIES/HI-Medium/04-sequence-counts.sql" hi-sequence-counts
role5_query "$ROLE5_QUERIES/HI-Medium/05-sequence-values.sql" hi-sequence-values
role5_query "$ROLE5_QUERIES/HI-Medium/06-sequence-duplicates.sql" hi-sequence-duplicates
role5_query "$ROLE5_QUERIES/HI-Medium/07-sequence-labels.sql" hi-sequence-labels
role5_query "$ROLE5_QUERIES/HI-Medium/09-paths.sql" hi-paths
```

Expect schema/duplicate checks to return zero data rows; 28 PASS dates; 56
date-length count rows matching the manifest; zero `bad_*` counters; all file
paths bound to the accepted version. Inspect after each query. Do not let a
zero-error empty result substitute for the expected manifest count.

Choose **either** the full lineage query or complete owner-date chunks:

```bash
# Full strategy (one expensive query, only if its scope/budget is approved):
role5_query "$ROLE5_QUERIES/HI-Medium/08-lineage-all.sql" hi-lineage-all

# Alternative chunk strategy: run each date separately, September 1 through 28.
# Example first chunk; do not treat it alone as full acceptance.
role5_query "$ROLE5_QUERIES/HI-Medium/lineage/2022-09-01.sql" hi-lineage-2022-09-01
```

No automatic 28-query loop is supplied: review each result/cost before the next.
Use all 28 rows in [RESULTS_CHECKLIST.md](RESULTS_CHECKLIST.md) to track chunks.
Require all lineage error counts zero and members = 2*pairs + 3*triples for the
matching date/full scope. Sum chunk member counts and verify no dates skipped.

## 7. LI after its full publication

Use fresh actual LI IDs and a newly confirmed stable window; repeat the metadata/
receipt block with `ROLE5_DATASET='LI-Medium'`, actual LI run/attempt/Glue IDs,
and the LI publication reference. A continuing HI query must not overlap LI
publication. Do not reuse HI source fingerprints, receipts or pause approvals
outside their agreed scope/time.

Run separately, after approvals and each preceding check passes:

```bash
role5_query "$ROLE5_QUERIES/LI-Medium/02-source-summary.sql" li-source-summary
role5_query "$ROLE5_QUERIES/LI-Medium/03-source-daily.sql" li-source-daily
role5_query "$ROLE5_QUERIES/00-schema.sql" li-schema
role5_query "$ROLE5_QUERIES/LI-Medium/01-partitions.sql" li-partitions
role5_query "$ROLE5_QUERIES/LI-Medium/04-sequence-counts.sql" li-sequence-counts
role5_query "$ROLE5_QUERIES/LI-Medium/05-sequence-values.sql" li-sequence-values
role5_query "$ROLE5_QUERIES/LI-Medium/06-sequence-duplicates.sql" li-sequence-duplicates
role5_query "$ROLE5_QUERIES/LI-Medium/07-sequence-labels.sql" li-sequence-labels
role5_query "$ROLE5_QUERIES/LI-Medium/09-paths.sql" li-paths
# Choose this full query OR every date chunk September 1 through 27:
role5_query "$ROLE5_QUERIES/LI-Medium/08-lineage-all.sql" li-lineage-all
# Alternative first chunk, followed by all other dates after review:
role5_query "$ROLE5_QUERIES/LI-Medium/lineage/2022-09-01.sql" li-lineage-2022-09-01
```

LI source: **31,251,469 transactions, 16,041 positive labels, 27 dates**,
September 1–27. Expect 27 PASS partitions, 54 date-length count rows, unique IDs,
zero value/lineage errors and exact manifest reconciliation. Both lengths retain
all valid branches and labels. After both publications pass and metadata is stable:

```bash
ROLE5_STABLE_OUTPUT_REF='<actual confirmed both-dataset stable acceptance window>'
role5_query "$ROLE5_QUERIES/99-both-datasets-partitions.sql" both-partitions
```

Expect exactly 55 PASS dates. Reconcile final source and publication inventories
across the validation window, finish the compact results and handoff, and obtain
explicit reader-release agreement. No Catalog switch, repair, deletion, Glue
start or generation/profiler invocation is included in these instructions.
