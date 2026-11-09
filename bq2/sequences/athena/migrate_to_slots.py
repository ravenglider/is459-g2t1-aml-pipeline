"""One-time switch from the plain table bq2_sequences to the two-copy layout. Run in CloudShell:

    python3 migrate_to_slots.py

Before: table bq2_sequences   (data in s3://is459-g2t1-aml-processed-bucket/bq2_sequences/)
After:  table bq2_sequences_a (same files, same partitions, nothing copied or rebuilt)
        view  bq2_sequences = SELECT * FROM bq2_sequences_a   <- what everyone queries, unchanged name

Safe to rerun: if bq2_sequences is already a view it only reports which copy is live.
Refuses to start while a pipeline execution or an Athena query is running. Row counts are compared
before and after; on any mismatch or error it puts the original table back.
"""
import sys
import time

import boto3

REGION = "us-east-1"
DB = "is459_g2t1_aml"
VIEW, SLOT_A = "bq2_sequences", "bq2_sequences_a"
LOCATION = "s3://is459-g2t1-aml-processed-bucket/bq2_sequences/"
STATE_MACHINE = "arn:aws:states:us-east-1:536697253251:stateMachine:is459-g2t1-pipeline"
WORKGROUP = "primary"
TABLE_KEYS = ["Description", "Owner", "Retention", "StorageDescriptor", "PartitionKeys", "TableType", "Parameters"]
PART_KEYS = ["Values", "StorageDescriptor", "Parameters"]


def athena_sql(athena, sql):
    qid = athena.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": DB},
                                       WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if q["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if q["Status"]["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena query failed ({qid}): {q['Status'].get('StateChangeReason')}")
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return qid, [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows[1:]]


def counts(athena, name):
    qid, rows = athena_sql(athena, f"SELECT dataset, sequence_length, COUNT(*) FROM {DB}.{name} "
                                   "GROUP BY dataset, sequence_length ORDER BY dataset, sequence_length")
    return qid, rows


def all_partitions(glue, name):
    out = []
    for page in glue.get_paginator("get_partitions").paginate(DatabaseName=DB, TableName=name):
        out += page["Partitions"]
    return out


def create_table(glue, name, table, partitions):
    glue.create_table(DatabaseName=DB, TableInput={"Name": name, **{k: table[k] for k in TABLE_KEYS if k in table}})
    inputs = [{k: p[k] for k in PART_KEYS if k in p} for p in partitions]
    for i in range(0, len(inputs), 100):
        resp = glue.batch_create_partition(DatabaseName=DB, TableName=name, PartitionInputList=inputs[i:i + 100])
        if resp.get("Errors"):
            raise RuntimeError(f"Partition copy failed for {name}: {resp['Errors'][:2]}")


def busy(sfn, athena):
    running = sfn.list_executions(stateMachineArn=STATE_MACHINE, statusFilter="RUNNING")["executions"]
    ids = athena.list_query_executions(WorkGroup=WORKGROUP, MaxResults=50)["QueryExecutionIds"]
    qs = athena.batch_get_query_execution(QueryExecutionIds=ids)["QueryExecutions"] if ids else []
    return [e["name"] for e in running], [q["QueryExecutionId"] for q in qs if q["Status"]["State"] in ("RUNNING", "QUEUED")]


def migrate(glue, athena, sfn, log=print):
    execs, queries = busy(sfn, athena)
    if execs or queries:
        log(f"Not now: pipeline executions running {execs}, Athena queries running {queries}. Try again when they finish.")
        return 1
    try:
        t = glue.get_table(DatabaseName=DB, Name=VIEW)["Table"]
    except glue.exceptions.EntityNotFoundException:
        log(f"{VIEW} does not exist. Nothing to migrate: the next pipeline run builds copy A and creates the view.")
        return 0
    if t.get("TableType") == "VIRTUAL_VIEW":
        log(f"Already migrated: {VIEW} is a view. The pipeline's plan step reports which copy is live.")
        return 0
    loc = t["StorageDescriptor"]["Location"].rstrip("/") + "/"
    if loc != LOCATION:
        log(f"Stopping: {VIEW} is stored at {loc}, expected {LOCATION}.")
        return 1
    try:
        glue.get_table(DatabaseName=DB, Name=SLOT_A)
        log(f"Stopping: {SLOT_A} already exists. Check it in Athena before going further.")
        return 1
    except glue.exceptions.EntityNotFoundException:
        pass

    parts = all_partitions(glue, VIEW)
    q0, before = counts(athena, VIEW)
    log(f"{VIEW}: {len(parts)} partitions. Counts (query {q0}): {before}")

    create_table(glue, SLOT_A, t, parts)
    q1, after_a = counts(athena, SLOT_A)
    if after_a != before or len(all_partitions(glue, SLOT_A)) != len(parts):
        glue.delete_table(DatabaseName=DB, Name=SLOT_A)
        log(f"Stopping: {SLOT_A} does not match (query {q1}): {after_a}. Removed it, {VIEW} untouched.")
        return 1
    log(f"{SLOT_A} created on the same files, counts match (query {q1}).")

    glue.delete_table(DatabaseName=DB, Name=VIEW)  # removes the catalog entry only, the S3 files stay
    try:
        q2, _ = athena_sql(athena, f"CREATE OR REPLACE VIEW {DB}.{VIEW} AS SELECT * FROM {DB}.{SLOT_A}")
        q3, via_view = counts(athena, VIEW)
        if via_view != before:
            raise RuntimeError(f"view counts differ: {via_view}")
    except Exception as e:  # put the original table back
        try:
            glue.delete_table(DatabaseName=DB, Name=VIEW)
        except glue.exceptions.EntityNotFoundException:
            pass
        create_table(glue, VIEW, t, parts)
        log(f"Stopping: {e}. Restored the original {VIEW} table.")
        return 1
    log(f"Done. {VIEW} is now a view on {SLOT_A} (create {q2}, check {q3}). Counts unchanged: {via_view}")
    return 0


if __name__ == "__main__":
    sys.exit(migrate(boto3.client("glue", region_name=REGION), boto3.client("athena", region_name=REGION),
                     boto3.client("stepfunctions", region_name=REGION)))
