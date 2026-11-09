"""Run every statement of a generated .sql file in Athena, in order, one at a time.

For manual checks only. The production table is built by the Step Functions pipeline, which is the
only writer of bq2_sequences_a / bq2_sequences_b.

Built for AWS CloudShell (boto3 is preinstalled, credentials come from the console login):
    python3 run_athena.py 00_preflight.sql
    python3 run_athena.py 04_day_checks.sql --evidence evidence/acceptance.md   # also record results
    python3 run_athena.py 04_day_checks.sql --start 37   # resume at statement 37

Stops at the first failed statement and prints its number, so a rerun can resume there.
SELECT results (preflight, acceptance) are printed as rows.
"""
import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

DATABASE = "is459_g2t1_aml"
WORKGROUP = "primary"
REGION = "us-east-1"


def statements(path):
    text = open(path, encoding="utf-8").read()
    out = []
    for chunk in text.split(";"):
        code = [l for l in chunk.splitlines() if l.strip() and not l.strip().startswith("--")]
        if code:
            label = next((l[3:] for l in chunk.splitlines() if l.startswith("-- ") and "GENERATED" not in l
                          and "Run each statement" not in l), code[0][:80])
            is_query = code[0].lstrip().upper().startswith(("SELECT", "WITH"))
            out.append((label.strip(), chunk.strip(), is_query))
    return out


def run(client, sql):
    qid = client.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": DATABASE},
                                       WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        q = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = q["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return qid, q
        time.sleep(5)


def busy_queries(client):
    """Queries still running or queued in the workgroup (left over from an earlier run)."""
    ids = client.list_query_executions(WorkGroup=WORKGROUP, MaxResults=50)["QueryExecutionIds"]
    if not ids:
        return []
    qs = client.batch_get_query_execution(QueryExecutionIds=ids)["QueryExecutions"]
    return [q for q in qs if q["Status"]["State"] in ("RUNNING", "QUEUED")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sql_file")
    ap.add_argument("--start", type=int, default=1, help="statement number to start from (1-based)")
    ap.add_argument("--force", action="store_true", help="run even if other queries are still running")
    ap.add_argument("--evidence", help="append each statement's query id, timing and result rows to this markdown file")
    args = ap.parse_args()
    client = boto3.client("athena", region_name=REGION)
    busy = busy_queries(client)
    if busy and not args.force:
        print("Other Athena queries are still running or queued. Two builds at once corrupt the table.")
        for q in busy:
            print("   ", q["QueryExecutionId"], q["Status"]["State"], " ".join(q["Query"].split())[:90])
        print("Wait for them to finish, or stop them with:")
        print("    aws athena stop-query-execution --query-execution-id <id>")
        sys.exit(1)
    stmts = statements(args.sql_file)
    total_gb = 0.0
    ev = None
    if args.evidence:
        Path(args.evidence).parent.mkdir(parents=True, exist_ok=True)
        ev = open(args.evidence, "a", encoding="utf-8")
        ev.write(f"\n## {args.sql_file}, run {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC\n\n")
    for i, (label, sql, is_query) in enumerate(stmts, 1):
        if i < args.start:
            continue
        t0 = time.time()
        print(f"[{i}/{len(stmts)}] {label} ...", flush=True)
        qid, q = run(client, sql)
        stats = q.get("Statistics", {})
        gb = stats.get("DataScannedInBytes", 0) / 1e9
        total_gb += gb
        state = q["Status"]["State"]
        print(f"    {state} in {time.time() - t0:.0f}s, scanned {gb:.2f} GB (query id {qid})", flush=True)
        if ev:
            ev.write(f"**[{i}/{len(stmts)}] {label}**: {state} in {time.time() - t0:.0f}s, "
                     f"scanned {gb:.2f} GB, query id `{qid}`\n\n")
        if state != "SUCCEEDED":
            print("    reason:", q["Status"].get("StateChangeReason", "unknown"))
            print(f"Stopped. Fix the cause, then rerun with --start {i}")
            sys.exit(1)
        if is_query:
            rows = client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
            table = [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows[:50]]
            for r in table:
                print("    " + " | ".join(r))
            if ev and table:
                ev.write("| " + " | ".join(table[0]) + " |\n|" + "---|" * len(table[0]) + "\n")
                for r in table[1:]:
                    ev.write("| " + " | ".join(r) + " |\n")
                ev.write("\n")
                ev.flush()
    print(f"Done. Total scanned {total_gb:.2f} GB, about USD {total_gb / 1000 * 5:.2f}")
    if ev:
        ev.write(f"Total scanned {total_gb:.2f} GB, about USD {total_gb / 1000 * 5:.2f}\n")
        ev.close()


if __name__ == "__main__":
    main()
