"""is459-g2t1-bq2-control: safety controls for the BQ2 sequence build in Step Functions.

Deploy as a zip containing this file and generate_sql.py (handler lambda_function.lambda_handler).

Actions (event["action"]):
  lock    One pipeline run at a time. Fails with AnotherRunInProgress if an earlier execution of
          the state machine is still RUNNING (ties broken by name, so exactly one run proceeds).
  plan    Which copy is live (read from the bq2_sequences view) and which one to build (the other),
          plus the SQL the pipeline runs: source gate, create, structure gate, publish, and the day list.
  reset   Drop the build copy's table and empty its S3 folder. Refuses if that copy is live.
  render  SQL for one dataset-day: pairs insert, triples insert, count gate and lineage gate.

Production readers only ever see the bq2_sequences view. It is switched to the new copy only after
every gate passes, so a failed or partial build never replaces the live data.
"""
import base64
import json
import re

import boto3

import generate_sql as g

BUCKET = "is459-g2t1-aml-processed-bucket"
DB = g.DB
VIEW = g.VIEW
SLOTS = g.SLOTS  # {"a": {"table": "bq2_sequences_a", "prefix": "bq2_sequences"}, "b": {...}}
DAYS = {(d["dataset"], d["day"]) for d in g.all_days()}

_clients = {}


def client(name):
    if name not in _clients:
        _clients[name] = boto3.client(name)
    return _clients[name]


class AnotherRunInProgress(Exception):
    pass


class SlotError(Exception):
    pass


# ---------------------------------------------------------------- lock

def lock(event):
    me = event["execution_arn"]
    sm = event["state_machine_arn"]
    running = []
    for page in client("stepfunctions").get_paginator("list_executions").paginate(
            stateMachineArn=sm, statusFilter="RUNNING"):
        running += page["executions"]
    mine = next((e for e in running if e["executionArn"] == me), None)
    if mine is None:  # listing can lag a new execution by a moment
        d = client("stepfunctions").describe_execution(executionArn=me)
        mine = {"executionArn": me, "name": d["name"], "startDate": d["startDate"]}
    key = (mine["startDate"], mine["name"])
    earlier = [e for e in running if e["executionArn"] != me and (e["startDate"], e["name"]) < key]
    if earlier:
        names = ", ".join(f"{e['name']} (started {e['startDate']})" for e in earlier)
        raise AnotherRunInProgress(f"Another pipeline execution is still running: {names}. "
                                   "This run stopped before touching any data. Start it again when that one finishes.")
    return {"lock": "ok", "other_running_started_later": len(running) - 1}


# ---------------------------------------------------------------- slots

def view_sql(table):
    """The SQL text of an Athena view as stored in the Glue Data Catalog."""
    text = table.get("ViewOriginalText", "")
    m = re.search(r"/\* Presto View: (\S+) \*/", text)
    if m:
        return json.loads(base64.b64decode(m.group(1)))["originalSql"]
    return text


def live_slot():
    """'a', 'b', or None when bq2_sequences does not exist yet. Raises if it is a plain table."""
    glue = client("glue")
    try:
        t = glue.get_table(DatabaseName=DB, Name=VIEW)["Table"]
    except glue.exceptions.EntityNotFoundException:
        return None
    if t.get("TableType") != "VIRTUAL_VIEW":
        raise SlotError(f"{VIEW} is still a plain table, not a view. Run migrate_to_slots.py once "
                        "(bq2/sequences/athena) before the first slot-based pipeline run.")
    sql = view_sql(t)
    hits = [k for k, v in SLOTS.items() if re.search(rf"\b{v['table']}\b", sql)]
    if len(hits) != 1:
        raise SlotError(f"Cannot tell which copy {VIEW} points to. View SQL: {sql[:300]}")
    return hits[0]


def plan(event):
    live = live_slot()
    target = "b" if live == "a" else "a"
    t = SLOTS[target]
    return {
        "live": live or "none",
        "target": target,
        "target_table": t["table"],
        "days": g.all_days(),
        "source_sql_before": g.source_gate_query("before build"),
        "source_sql_after": g.source_gate_query("after build"),
        "create_sql": g.create_empty_sql(t["table"], t["prefix"]),
        "structure_sql": g.structure_gate_query(t["table"]),
        "publish_sql": g.publish_sql(t["table"]),
    }


def clear_prefix(prefix):
    s3 = client("s3")
    deleted = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            resp = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys, "Quiet": True})
            if resp.get("Errors"):
                raise RuntimeError(f"Failed to delete some objects under {prefix}: {resp['Errors'][:3]}")
            deleted += len(keys)
    return deleted


def reset(event):
    target = event["target"]
    if target not in SLOTS:
        raise SlotError(f"Unknown copy {target!r}")
    live = live_slot()
    if target == live:
        raise SlotError(f"Refusing to reset copy {target}: {VIEW} currently points to it.")
    t = SLOTS[target]
    glue = client("glue")
    try:
        glue.delete_table(DatabaseName=DB, Name=t["table"])
        dropped = True
    except glue.exceptions.EntityNotFoundException:
        dropped = False
    deleted = clear_prefix(t["prefix"] + "/")
    return {"target": target, "table_dropped": dropped, "objects_deleted": deleted, "live": live or "none"}


def render(event):
    ds, day, table = event["dataset"], event["day"], event["target_table"]
    if (ds, day) not in DAYS:
        raise ValueError(f"Unknown dataset-day {ds} {day}")
    if table not in {v["table"] for v in SLOTS.values()}:
        raise ValueError(f"Unknown table {table}")
    return {"pairs_sql": g.build_sql(table, ds, day, 2),
            "triples_sql": g.build_sql(table, ds, day, 3),
            "count_sql": g.day_count_gate_query(table, ds, day),
            "lineage_sql": g.day_lineage_gate_query(table, ds, day)}


ACTIONS = {"lock": lock, "plan": plan, "reset": reset, "render": render}


def lambda_handler(event, context):
    result = ACTIONS[event["action"]](event)
    if event["action"] != "render":
        print(json.dumps({k: v for k, v in result.items() if "sql" not in k and k != "days"}, default=str))
    return result
