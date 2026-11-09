"""Local tests for the control Lambda (fake AWS clients) and the source gate logic (DuckDB).
    python3 test_control.py   -> prints ALL PASS
"""
import base64
import json
import sys
import types
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import sqlglot

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / "control_lambda"))
sys.modules.setdefault("boto3", types.ModuleType("boto3"))  # the Lambda imports boto3; fakes replace it

import generate_sql as g  # noqa: E402
import lambda_function as lf  # noqa: E402

results = []


def check(name, cond):
    results.append(cond)
    print(f"   {'OK  ' if cond else 'FAIL'} {name}")


def raises(fn, exc):
    try:
        fn()
    except exc as e:
        return str(e)
    return None


# ---------------------------------------------------------------- fakes

class NotFound(Exception):
    pass


class FakeGlue:
    class exceptions:
        EntityNotFoundException = NotFound

    def __init__(self, tables):
        self.tables = tables  # name -> Table dict
        self.deleted = []

    def get_table(self, DatabaseName, Name):
        if Name not in self.tables:
            raise NotFound(Name)
        return {"Table": self.tables[Name]}

    def delete_table(self, DatabaseName, Name):
        if Name not in self.tables:
            raise NotFound(Name)
        self.deleted.append(Name)
        del self.tables[Name]


class FakeS3:
    def __init__(self, keys):
        self.keys = set(keys)

    def get_paginator(self, _):
        outer = self

        class P:
            def paginate(self, Bucket, Prefix):
                ks = sorted(k for k in outer.keys if k.startswith(Prefix))
                for i in range(0, max(len(ks), 1), 1000):
                    yield {"Contents": [{"Key": k} for k in ks[i:i + 1000]]} if ks else {}
        return P()

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.keys.discard(o["Key"])
        return {}


class FakeSfn:
    def __init__(self, running):
        self.running = running

    def get_paginator(self, _):
        outer = self

        class P:
            def paginate(self, stateMachineArn, statusFilter):
                yield {"executions": outer.running}
        return P()

    def describe_execution(self, executionArn):
        e = next(x for x in self.running if x["executionArn"] == executionArn)
        return {"name": e["name"], "startDate": e["startDate"]}


def athena_view(sql):
    payload = base64.b64encode(json.dumps({"originalSql": sql, "catalog": "awsdatacatalog"}).encode()).decode()
    return {"Name": "bq2_sequences", "TableType": "VIRTUAL_VIEW",
            "ViewOriginalText": f"/* Presto View: {payload} */", "ViewExpandedText": "/* Presto View */"}


def use(glue=None, s3=None, sfn=None):
    lf._clients.clear()
    lf._clients.update({k: v for k, v in (("glue", glue), ("s3", s3), ("stepfunctions", sfn)) if v})


# ---------------------------------------------------------------- tests

def test_slots():
    print("copies (A/B) and reset safety:")
    view_a = athena_view("SELECT * FROM is459_g2t1_aml.bq2_sequences_a")
    glue = FakeGlue({"bq2_sequences": view_a, "bq2_sequences_a": {}, "bq2_sequences_b": {}})
    keys = ["bq2_sequences/dataset=HI-Medium/start_date=2022-09-01/f1", "bq2_sequences_b/dataset=HI-Medium/x",
            "bq2_sequences_b/dataset=LI-Medium/y", "bq2_sequences_check/z", "bq1_corridors/w"]
    s3 = FakeS3(keys)
    use(glue, s3)
    p = lf.lambda_handler({"action": "plan"}, None)
    check("view -> A is live, plan builds B", (p["live"], p["target"], p["target_table"]) == ("a", "b", "bq2_sequences_b"))
    check("create SQL writes only to bq2_sequences_b/", "external_location = 's3://is459-g2t1-aml-processed-bucket/bq2_sequences_b/'"
          in p["create_sql"] and p["create_sql"].startswith("-- bq2 create bq2_sequences_b") and p["create_sql"].endswith("WITH NO DATA"))
    check("publish switches the view to B", p["publish_sql"].endswith("CREATE OR REPLACE VIEW is459_g2t1_aml.bq2_sequences AS SELECT * FROM is459_g2t1_aml.bq2_sequences_b"))
    check("structure gate reads B", "FROM is459_g2t1_aml.bq2_sequences_b" in p["structure_sql"])
    check("55 dataset-days planned", len(p["days"]) == 55)
    msg = raises(lambda: lf.lambda_handler({"action": "reset", "target": "a"}, None), lf.SlotError)
    check("reset refuses the live copy A", msg is not None and "currently points to it" in msg and "bq2_sequences_a" in glue.tables)
    r = lf.lambda_handler({"action": "reset", "target": "b"}, None)
    check("reset B drops only bq2_sequences_b", glue.deleted == ["bq2_sequences_b"])
    check("reset B empties only bq2_sequences_b/", r["objects_deleted"] == 2 and s3.keys == {keys[0], keys[3], keys[4]})
    check("reset B again is harmless", lf.lambda_handler({"action": "reset", "target": "b"}, None)["table_dropped"] is False)
    check("unknown copy refused", raises(lambda: lf.lambda_handler({"action": "reset", "target": "c"}, None), lf.SlotError) is not None)

    use(FakeGlue({"bq2_sequences": athena_view("SELECT * FROM is459_g2t1_aml.bq2_sequences_b")}), FakeS3([]))
    p = lf.lambda_handler({"action": "plan"}, None)
    check("view -> B is live, plan builds A (bq2_sequences/)", (p["live"], p["target"]) == ("b", "a")
          and "'s3://is459-g2t1-aml-processed-bucket/bq2_sequences/'" in p["create_sql"])

    use(FakeGlue({}), FakeS3([]))
    p = lf.lambda_handler({"action": "plan"}, None)
    check("no view yet: build A", (p["live"], p["target"]) == ("none", "a"))

    use(FakeGlue({"bq2_sequences": {"TableType": "EXTERNAL_TABLE"}}), FakeS3([]))
    msg = raises(lambda: lf.lambda_handler({"action": "plan"}, None), lf.SlotError)
    check("plain table (not migrated) stops the run", msg is not None and "migrate_to_slots.py" in msg)
    msg = raises(lambda: lf.lambda_handler({"action": "reset", "target": "a"}, None), lf.SlotError)
    check("plain table: reset also refuses", msg is not None)

    use(FakeGlue({"bq2_sequences": athena_view("SELECT * FROM x.bq2_sequences_a UNION ALL SELECT * FROM x.bq2_sequences_b")}), FakeS3([]))
    check("ambiguous view stops the run", raises(lambda: lf.lambda_handler({"action": "plan"}, None), lf.SlotError) is not None)


def test_lock():
    print("one execution at a time:")
    t0 = datetime(2026, 11, 9, 4, 0)
    ex = lambda n, dt: {"executionArn": f"arn:x:{n}", "name": n, "startDate": t0 + timedelta(seconds=dt)}
    ev = lambda n: {"action": "lock", "execution_arn": f"arn:x:{n}", "state_machine_arn": "arn:sm"}
    use(sfn=FakeSfn([ex("first", 0)]))
    check("alone: proceeds", lf.lambda_handler(ev("first"), None)["lock"] == "ok")
    use(sfn=FakeSfn([ex("first", 0), ex("second", 30)]))
    check("second while first runs: stops", raises(lambda: lf.lambda_handler(ev("second"), None), lf.AnotherRunInProgress) is not None)
    check("first is not blocked by a later one", lf.lambda_handler(ev("first"), None)["lock"] == "ok")
    use(sfn=FakeSfn([ex("bbb", 0), ex("aaa", 0)]))
    a_ok = raises(lambda: lf.lambda_handler(ev("aaa"), None), lf.AnotherRunInProgress) is None
    b_ok = raises(lambda: lf.lambda_handler(ev("bbb"), None), lf.AnotherRunInProgress) is None
    check("same start second: exactly one proceeds", (a_ok, b_ok) == (True, False))


def test_render():
    print("per-day SQL:")
    use(FakeGlue({}), FakeS3([]))
    r = lf.lambda_handler({"action": "render", "dataset": "LI-Medium", "day": "2022-09-27",
                           "target_table": "bq2_sequences_b"}, None)
    check("pairs/triples/gate for the requested copy and day",
          r["pairs_sql"] == g.build_sql("bq2_sequences_b", "LI-Medium", "2022-09-27", 2)
          and r["triples_sql"] == g.build_sql("bq2_sequences_b", "LI-Medium", "2022-09-27", 3)
          and r["count_sql"] == g.day_count_gate_query("bq2_sequences_b", "LI-Medium", "2022-09-27")
          and r["lineage_sql"] == g.day_lineage_gate_query("bq2_sequences_b", "LI-Medium", "2022-09-27")
          and "INSERT INTO is459_g2t1_aml.bq2_sequences_b" in r["pairs_sql"])
    check("day outside the data refused", raises(lambda: lf.lambda_handler(
        {"action": "render", "dataset": "LI-Medium", "day": "2022-09-28", "target_table": "bq2_sequences_b"}, None), ValueError) is not None)
    check("writing to the view refused", raises(lambda: lf.lambda_handler(
        {"action": "render", "dataset": "HI-Medium", "day": "2022-09-01", "target_table": "bq2_sequences"}, None), ValueError) is not None)


def test_source_gate():
    print("source gate (completeness):")
    con = duckdb.connect()
    con.execute("CREATE SCHEMA is459_g2t1_aml")
    con.execute("CREATE TABLE is459_g2t1_aml.transactions_clean (txn_id BIGINT, dataset VARCHAR, txn_date VARCHAR)")
    rows = []
    for ds, (f, t) in g.DATE_RANGES.items():
        for i, d in enumerate(g.dates(f, t)):
            rows += [((1 if ds.startswith("HI") else 2) * 10_000 + i * 10 + k, ds, d) for k in range(3)]
    con.executemany("INSERT INTO is459_g2t1_aml.transactions_clean VALUES (?, ?, ?)", rows)
    saved = {k: dict(v) for k, v in g.SOURCE_EXPECTED.items()}
    for ds in g.SOURCE_EXPECTED:
        g.SOURCE_EXPECTED[ds]["rows"] = sum(1 for r in rows if r[1] == ds)

    def gate():
        sql = sqlglot.transpile(g.source_check_query(fingerprint_expr="'fp'"), read="trino", write="duckdb")[0]
        return con.execute(sql).fetchone()

    check("complete source: ok = true", gate()[0] == "true")
    con.execute("DELETE FROM is459_g2t1_aml.transactions_clean WHERE dataset = 'LI-Medium' AND txn_date = '2022-09-14'")
    check("a missing day: ok = false", gate()[0] == "false")
    con.execute("INSERT INTO is459_g2t1_aml.transactions_clean VALUES (20131, 'LI-Medium', '2022-09-14'), "
                "(20132, 'LI-Medium', '2022-09-14'), (20133, 'LI-Medium', '2022-09-14')")
    check("restored: ok = true", gate()[0] == "true")
    con.execute("UPDATE is459_g2t1_aml.transactions_clean SET txn_id = 10000 WHERE txn_id = 10001")
    check("a duplicate txn_id: ok = false", gate()[0] == "false")
    con.execute("UPDATE is459_g2t1_aml.transactions_clean SET txn_id = 10001 WHERE txn_id = 10000 AND txn_date = '2022-09-01' "
                "AND rowid = (SELECT max(rowid) FROM is459_g2t1_aml.transactions_clean WHERE txn_id = 10000)")
    check("fixed: ok = true", gate()[0] == "true")
    con.execute("INSERT INTO is459_g2t1_aml.transactions_clean VALUES (99999, 'HI-Medium', '2022-09-29')")
    check("an extra row / day outside the contract: ok = false", gate()[0] == "false")
    check("detail explains what was found", "Found HI: " in gate()[1])
    g.SOURCE_EXPECTED.clear()
    g.SOURCE_EXPECTED.update(saved)
    check("real query keeps the file fingerprint", '"$path"' in g.source_check_query() and "$file_modified_time" in g.source_check_query())


class MigGlue(FakeGlue):
    def __init__(self, tables, partitions):
        super().__init__(tables)
        self.parts = partitions  # name -> list

    def get_paginator(self, _):
        outer = self

        class P:
            def paginate(self, DatabaseName, TableName):
                yield {"Partitions": list(outer.parts.get(TableName, []))}
        return P()

    def create_table(self, DatabaseName, TableInput):
        self.tables[TableInput["Name"]] = {k: v for k, v in TableInput.items() if k != "Name"}

    def batch_create_partition(self, DatabaseName, TableName, PartitionInputList):
        self.parts.setdefault(TableName, []).extend(PartitionInputList)
        return {}

    def delete_table(self, DatabaseName, Name):
        super().delete_table(DatabaseName, Name)
        self.parts.pop(Name, None)


class MigAthena:
    """Counts come from the partitions the fake catalog has for the queried table (or the view's target)."""
    def __init__(self, glue, fail_view=False):
        self.glue, self.fail_view, self.q = glue, fail_view, {}

    def list_query_executions(self, **k):
        return {"QueryExecutionIds": []}

    def start_query_execution(self, QueryString, **k):
        qid = f"q{len(self.q) + 1}"
        self.q[qid] = QueryString
        if QueryString.startswith("CREATE OR REPLACE VIEW"):
            if self.fail_view:
                raise RuntimeError("simulated view failure")
            self.glue.tables["bq2_sequences"] = athena_view(QueryString.split(" AS ", 1)[1])
        return {"QueryExecutionId": qid}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    def get_query_results(self, QueryExecutionId):
        sql = self.q[QueryExecutionId]
        rows = [{"Data": [{"VarCharValue": "h"}]}]
        if sql.startswith("SELECT dataset"):
            name = sql.split("FROM is459_g2t1_aml.")[1].split()[0]
            t = self.glue.tables[name]
            if t.get("TableType") == "VIRTUAL_VIEW":
                name = lf.view_sql(t).split("is459_g2t1_aml.")[1]
            n = len(self.glue.parts.get(name, []))
            rows.append({"Data": [{"VarCharValue": "HI-Medium"}, {"VarCharValue": "2"}, {"VarCharValue": str(n * 1000)}]})
        return {"ResultSet": {"Rows": rows}}


def test_migration():
    import migrate_to_slots as m
    print("one-time migration to two copies:")
    sd = {"Location": "s3://is459-g2t1-aml-processed-bucket/bq2_sequences/"}
    table = {"TableType": "EXTERNAL_TABLE", "StorageDescriptor": sd, "PartitionKeys": [{"Name": "dataset"}, {"Name": "start_date"}],
             "Parameters": {"classification": "parquet"}, "CreateTime": "x", "DatabaseName": "is459_g2t1_aml"}
    parts = [{"Values": ["HI-Medium", f"2022-09-{i:02d}"], "StorageDescriptor": sd, "CreationTime": "x"} for i in range(1, 29)]
    sfn = FakeSfn([])
    sfn.list_executions = lambda **k: {"executions": []}
    glue = MigGlue({"bq2_sequences": dict(table)}, {"bq2_sequences": list(parts)})
    logs = []
    rc = m.migrate(glue, MigAthena(glue), sfn, logs.append)
    view = glue.tables.get("bq2_sequences", {})
    check("table becomes view on bq2_sequences_a", rc == 0 and view.get("TableType") == "VIRTUAL_VIEW"
          and "bq2_sequences_a" in lf.view_sql(view))
    check("copy A keeps the same files and all 28 partitions",
          glue.tables["bq2_sequences_a"]["StorageDescriptor"] == sd and len(glue.parts["bq2_sequences_a"]) == 28)
    use(glue, FakeS3([]))
    check("control Lambda then sees A live and plans B", lf.lambda_handler({"action": "plan"}, None)["target"] == "b")
    rc = m.migrate(glue, MigAthena(glue), sfn, logs.append)
    check("rerun is harmless", rc == 0 and "Already migrated" in logs[-1])

    glue = MigGlue({"bq2_sequences": dict(table)}, {"bq2_sequences": list(parts)})
    rc = m.migrate(glue, MigAthena(glue, fail_view=True), sfn, logs.append)
    check("view failure: original table restored", rc == 1 and glue.tables["bq2_sequences"]["TableType"] == "EXTERNAL_TABLE"
          and len(glue.parts["bq2_sequences"]) == 28 and "Restored" in logs[-1])

    sfn.list_executions = lambda **k: {"executions": [{"name": "run1"}]}
    glue = MigGlue({"bq2_sequences": dict(table)}, {"bq2_sequences": list(parts)})
    rc = m.migrate(glue, MigAthena(glue), sfn, logs.append)
    check("refuses while the pipeline runs", rc == 1 and glue.tables["bq2_sequences"]["TableType"] == "EXTERNAL_TABLE")


def test_evidence():
    import collect_evidence as ce
    print("evidence from execution history:")
    q = lambda qid, sql, gb: {"QueryExecution": {"QueryExecutionId": qid, "Query": sql,
                                                 "Statistics": {"DataScannedInBytes": gb * 1e9, "TotalExecutionTimeInMillis": 9000}}}
    ev = lambda out, rtype="athena", res="startQueryExecution.sync": {
        "type": "TaskSucceeded", "taskSucceededEventDetails": {"resourceType": rtype, "resource": res, "output": json.dumps(out)}}
    events = [ev({"Payload": {"live": "a", "target": "b", "target_table": "bq2_sequences_b"}}, "lambda", "invoke"),
              ev(q("g1", g.source_gate_query("before build"), 0.5)),
              ev(q("b1", g.build_sql("bq2_sequences_b", "HI-Medium", "2022-09-01", 2), 0.2)),
              ev(q("g2", g.day_count_gate_query("bq2_sequences_b", "HI-Medium", "2022-09-01"), 4.0)),
              ev(q("x1", "CREATE TABLE is459_g2t1_aml.bq1_corridors_ranked AS SELECT 1", 1.0))]

    class S:
        def describe_execution(self, executionArn):
            return {"name": "run7", "status": "SUCCEEDED", "startDate": "t0", "stopDate": "t1"}

        def get_paginator(self, _):
            class P:
                def paginate(self, **k):
                    yield {"events": events}
            return P()

    class A:
        def get_query_results(self, QueryExecutionId):
            return {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": "ok"}, {"VarCharValue": "expected_pairs"}]},
                                           {"Data": [{"VarCharValue": "true"}, {"VarCharValue": "14679196"}]}]}}

    name, text = ce.report(S(), A(), "arn:exec:run7")
    check("names gates and builds, skips non-BQ2 queries", "**gate count HI-Medium 2022-09-01**: ok = **true**" in text
          and "| build HI-Medium 2022-09-01 pairs | `b1`" in text and "bq1_corridors" not in text and "`x1`" not in text)
    check("records copy, status and cost", "Built copy `bq2_sequences_b` while `a` was live" in text
          and "**SUCCEEDED**" in text and "4.70 GB" in text and "| 14679196 |" in text)


if __name__ == "__main__":
    test_slots()
    test_migration()
    test_evidence()
    test_lock()
    test_render()
    test_source_gate()
    print("ALL PASS" if all(results) else "FAIL")
    raise SystemExit(0 if all(results) else 1)
