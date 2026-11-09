"""Write the BQ2 evidence for one pipeline execution, straight from its Step Functions history.

    python3 collect_evidence.py <execution ARN>      -> evidence/pipeline_<execution name>.md

Every BQ2 query starts with a label comment ("-- bq2 build ...", "-- bq2 gate ..."), so the report
names each query, gives its Athena query id, data scanned and time, and for every gate the full
result row (ok plus each count). Nothing is rerun, so this costs nothing.
"""
import json
import sys
from pathlib import Path

import boto3

REGION = "us-east-1"


def history(sfn, arn):
    events = []
    for page in sfn.get_paginator("get_execution_history").paginate(executionArn=arn, includeExecutionData=True):
        events += page["events"]
    return events


def bq2_queries(events):
    """(label, QueryExecution) for every successful BQ2 Athena task, in order."""
    out = []
    for e in events:
        d = e.get("taskSucceededEventDetails")
        if not d or d.get("resourceType") != "athena" or not d.get("resource", "").startswith("startQueryExecution"):
            continue
        q = json.loads(d.get("output") or "{}").get("QueryExecution", {})
        first = (q.get("Query") or "").split("\n", 1)[0]
        if first.startswith("-- bq2 "):
            out.append((first[7:], q))
    return out


def plan_output(events):
    for e in events:
        d = e.get("taskSucceededEventDetails")
        if d and d.get("resourceType") == "lambda":
            payload = json.loads(d.get("output") or "{}").get("Payload", {})
            if isinstance(payload, dict) and "target_table" in payload and "live" in payload:
                return payload
    return None


def failures(events):
    out = []
    for e in events:
        for key in ("executionFailedEventDetails", "taskFailedEventDetails", "mapRunFailedEventDetails"):
            d = e.get(key)
            if d:
                out.append(f"{e['type']}: {d.get('error', '')} {d.get('cause', '')[:500]}")
    return out


def report(sfn, athena, arn):
    ex = sfn.describe_execution(executionArn=arn)
    events = history(sfn, arn)
    qs = bq2_queries(events)
    plan = plan_output(events)
    lines = [f"# BQ2 pipeline evidence: {ex['name']}", "",
             f"- Execution: `{arn}`",
             f"- Status: **{ex['status']}**, started {ex['startDate']}, stopped {ex.get('stopDate', 'still running')}"]
    if plan:
        lines.append(f"- Built copy `{plan['target_table']}` while `{plan['live']}` was live")
    builds = [(l, q) for l, q in qs if l.startswith("build ")]
    scanned = sum(q.get("Statistics", {}).get("DataScannedInBytes", 0) for _, q in qs) / 1e9
    lines += [f"- BQ2 queries: {len(qs)} ({len(builds)} build inserts), {scanned:.2f} GB scanned, "
              f"about USD {scanned / 1000 * 5:.2f}", ""]

    lines += ["## Gates", "", "Each gate passes only if `ok` is true. Full result rows below.", ""]
    for label, q in qs:
        if not label.startswith("gate "):
            continue
        qid = q["QueryExecutionId"]
        rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        head = [c.get("VarCharValue", "") for c in rows[0]["Data"]]
        vals = [c.get("VarCharValue", "") for c in rows[1]["Data"]] if len(rows) > 1 else []
        ms = q.get("Statistics", {}).get("TotalExecutionTimeInMillis", 0)
        lines.append(f"**{label}**: ok = **{vals[0] if vals else '?'}**, query `{qid}`, "
                     f"{q.get('Statistics', {}).get('DataScannedInBytes', 0) / 1e9:.2f} GB, {ms / 1000:.0f}s")
        lines.append("")
        lines.append("| " + " | ".join(head[1:]) + " |")
        lines.append("|" + "---|" * (len(head) - 1))
        lines.append("| " + " | ".join(vals[1:]) + " |")
        lines.append("")

    lines += ["## Other BQ2 queries", "", "| Label | Query id | GB scanned | Seconds |", "|---|---|---|---|"]
    for label, q in qs:
        if label.startswith("gate "):
            continue
        st = q.get("Statistics", {})
        lines.append(f"| {label} | `{q['QueryExecutionId']}` | {st.get('DataScannedInBytes', 0) / 1e9:.2f} | "
                     f"{st.get('TotalExecutionTimeInMillis', 0) / 1000:.0f} |")
    fails = failures(events)
    if fails:
        lines += ["", "## Failures", ""] + [f"- {f}" for f in fails]
    return ex["name"], "\n".join(lines) + "\n"


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    name, text = report(boto3.client("stepfunctions", region_name=REGION),
                        boto3.client("athena", region_name=REGION), sys.argv[1])
    out = Path(__file__).resolve().parent / "evidence" / f"pipeline_{name}.md"
    out.parent.mkdir(exist_ok=True)
    out.write_text(text, encoding="utf-8")
    print(f"wrote {out}")
