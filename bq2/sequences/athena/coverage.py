"""Per-date completeness evidence for the live bq2_sequences view. Run in CloudShell:

    python3 coverage.py            -> evidence/coverage_<UTC timestamp>.md

For all 55 dataset-days: source transactions, chains expected from a fresh recount (the count gate),
and chains stored. A date with no stored partition is complete only if its recount is also 0.
Reads only the source and partition counts (no array columns): about 40 GB in total.
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import boto3

import generate_sql as g

REGION, WORKGROUP = "us-east-1", "primary"


def run(athena, sql):
    qid = athena.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": g.DB},
                                       WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if q["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)
    if q["Status"]["State"] != "SUCCEEDED":
        raise RuntimeError(f"{sql.splitlines()[0]} failed ({qid}): {q['Status'].get('StateChangeReason')}")
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return qid, [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows[1:]]


def coverage(athena, table="bq2_sequences", log=print):
    _, src = run(athena, f"SELECT dataset, txn_date, COUNT(*) FROM {g.SRC} WHERE dataset IN ('HI-Medium', 'LI-Medium') "
                          "GROUP BY dataset, txn_date")
    source = {(r[0], r[1]): int(r[2]) for r in src}
    days = g.all_days()

    def one(item):
        qid, rows = run(athena, g.day_count_gate_query(table, item["dataset"], item["day"]))
        ok, _, _, ep, ap, et, at = rows[0]
        log(f"  {item['dataset']} {item['day']}: ok={ok}")
        return item["dataset"], item["day"], ok, int(ep), int(ap), int(et), int(at), qid

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(one, days))
    return source, results


def report(source, results):
    now = datetime.now(timezone.utc)
    bad = [r for r in results if r[2] != "true"]
    empty = [r for r in results if r[3] == r[5] == 0 and r[2] == "true"]
    lines = [f"# bq2_sequences per-date coverage, {now:%Y-%m-%d %H:%M} UTC", "",
             "Expected = fresh recount from `transactions_clean` with the build rules. Stored = rows in the live "
             "`bq2_sequences` view. A date without a stored partition is complete only if expected is also 0.", "",
             f"**{len(results) - len(bad)} of {len(results)} dataset-days match.**"
             + (f" Mismatches: {', '.join(f'{r[0]} {r[1]}' for r in bad)}." if bad else ""), ""]
    if empty:
        lines += ["Dates with zero valid chains (no partition is written for an empty result): "
                  + ", ".join(f"{r[0]} {r[1]} ({source.get((r[0], r[1]), 0):,} source transactions)" for r in empty), ""]
    lines += ["| Dataset | Start date | Source txns | Expected pairs | Stored pairs | Expected triples | Stored triples | Match | Query id |",
              "|---|---|---:|---:|---:|---:|---:|---|---|"]
    tot = {}
    for ds, d, ok, ep, ap, et, at, qid in results:
        lines.append(f"| {ds} | {d} | {source.get((ds, d), 0):,} | {ep:,} | {ap:,} | {et:,} | {at:,} | {ok} | `{qid}` |")
        t = tot.setdefault(ds, [0, 0, 0, 0])
        for i, v in enumerate((ep, ap, et, at)):
            t[i] += v
    lines += ["", "| Dataset | Expected pairs | Stored pairs | Expected triples | Stored triples |", "|---|---:|---:|---:|---:|"]
    lines += [f"| {ds} | {t[0]:,} | {t[1]:,} | {t[2]:,} | {t[3]:,} |" for ds, t in tot.items()]
    return "\n".join(lines) + "\n", not bad


if __name__ == "__main__":
    src, res = coverage(boto3.client("athena", region_name=REGION))
    text, ok = report(src, res)
    out = Path(__file__).resolve().parent / "evidence" / f"coverage_{datetime.now(timezone.utc):%Y%m%dT%H%M}.md"
    out.parent.mkdir(exist_ok=True)
    out.write_text(text, encoding="utf-8")
    print(f"wrote {out}: {'all dates match' if ok else 'MISMATCHES, see the file'}")
    sys.exit(0 if ok else 1)
