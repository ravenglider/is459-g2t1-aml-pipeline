"""Patch the Step Functions definition with the gated, two-copy BQ2 sequence build.

    python3 build_asl.py   -> rewrites ingestion/stepfunctions/is459-g2t1-pipeline.asl.json

Pipeline start: PipelineLock (one execution at a time), then the existing ingestion and ETL.
BQ2 branch:
  Plan -> source gate (before) -> reset the copy that is NOT live -> create it empty
  -> Map over 55 dataset-days, 5 at a time: render SQL, pairs, triples, count gate, lineage gate (must pass)
  -> structure gate over the whole copy -> source gate (after: same contract, same files)
  -> publish: bq2_sequences view switches to the new copy -> Role 6.
Any failure stops before publish, so the live copy is untouched. Athena tasks are retried only
when Athena refuses to start a query (nothing written yet), never after a query has run.
Role 6 (after publish), SQL read from bq2/lift at build time:
  reset the 5 Role 6 tables -> bq2_lift -> trade-off stage 1 (CTAS + 3 INSERTs, in order)
  -> bq2_tradeoff -> bq2_robustness -> bq2_sankey -> lift gate (bq2/lift/pipeline_gate.sql).

    python3 build_asl.py --lift-only   -> also writes bq2/lift/lift_only.asl.json, a small
                                          state machine with just the Role 6 chain for a quick test
"""
import sys
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ASL = ROOT / "ingestion/stepfunctions/is459-g2t1-pipeline.asl.json"
LIFT = ROOT / "bq2/lift"
RESET = "is459-g2t1-reset-outputs"
LIFT_TABLES = ["bq2_lift", "bq2_tradeoff_flagged", "bq2_tradeoff", "bq2_robustness", "bq2_sankey"]
CONTROL = "is459-g2t1-bq2-control"
ATHENA = {"WorkGroup": "primary", "QueryExecutionContext": {"Database": "is459_g2t1_aml"},
          "ResultConfiguration": {"OutputLocation": "s3://is459-g2t1-aml-athena-bucket/results/"}}
ATHENA_RETRY = [{"ErrorEquals": ["Athena.TooManyRequestsException"], "IntervalSeconds": 30,
                 "MaxAttempts": 6, "BackoffRate": 1.5}]
LAMBDA_RETRY = [{"ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException",
                                 "Lambda.SdkClientException", "Lambda.TooManyRequestsException"],
                 "IntervalSeconds": 5, "MaxAttempts": 3, "BackoffRate": 2}]


def control(payload, result_path, nxt, comment, selector=True):
    s = {"Type": "Task", "Comment": comment, "Resource": "arn:aws:states:::lambda:invoke",
         "Parameters": {"FunctionName": CONTROL, "Payload": payload}, "Retry": LAMBDA_RETRY,
         "ResultPath": result_path, "Next": nxt}
    if selector and result_path:
        s["ResultSelector"] = {"out.$": "$.Payload"}
    return s


def athena(sql_path, result_path, nxt, comment, keep_id=False):
    s = {"Type": "Task", "Comment": comment, "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
         "Parameters": {"QueryString.$": sql_path, **ATHENA}, "Retry": ATHENA_RETRY,
         "ResultPath": result_path, "Next": nxt}
    if keep_id:
        s["ResultSelector"] = {"qid.$": "$.QueryExecution.QueryExecutionId"}
    return s


def gate(name, qid_path, result_path, ok_next, fail_name, extra=None):
    """getQueryResults -> Choice on the first column (ok). extra = more columns to keep {key: index}."""
    sel = {"ok.$": "$.ResultSet.Rows[1].Data[0].VarCharValue", "row.$": "$.ResultSet.Rows[1].Data"}
    for k, i in (extra or {}).items():
        sel[f"{k}.$"] = f"$.ResultSet.Rows[1].Data[{i}].VarCharValue"
    return {
        f"{name}_Result": {"Type": "Task", "Resource": "arn:aws:states:::athena:getQueryResults",
                           "Parameters": {"QueryExecutionId.$": qid_path}, "ResultSelector": sel,
                           "ResultPath": result_path, "Next": f"{name}_Ok?"},
        f"{name}_Ok?": {"Type": "Choice",
                        "Choices": [{"Variable": f"{result_path}.ok", "StringEquals": "true", "Next": ok_next}],
                        "Default": fail_name},
    }


def fail(error, cause_expr):
    return {"Type": "Fail", "Error": error, "CausePath": cause_expr}


def bq2_states():
    P = "$.bq2.plan.out"
    day_states = {
        "Day_Render": control({"action": "render", "dataset.$": "$.dataset", "day.$": "$.day",
                               "target_table.$": "$.target_table"}, "$.sql", "Day_Pairs",
                              "SQL for this dataset-day"),
        "Day_Pairs": athena("$.sql.out.pairs_sql", None, "Day_Triples", "Insert this day's pairs"),
        "Day_Triples": athena("$.sql.out.triples_sql", None, "Day_Count",
                              "Insert this day's triples (after pairs, so the partition is created once)"),
        "Day_Count": athena("$.sql.out.count_sql", "$.countq", "Day_Count_Result",
                            "Gate: stored pairs and triples = fresh recount from transactions_clean", keep_id=True),
        **gate("Day_Count", "$.countq.qid", "$.countres", "Day_Lineage", "Day_CountFailed"),
        "Day_CountFailed": fail("DayCountCheckFailed",
                                "States.Format('{} {} count gate failed: {}', $.dataset, $.day, States.JsonToString($.countres.row))"),
        "Day_Lineage": athena("$.sql.out.lineage_sql", "$.lineageq", "Day_Lineage_Result",
                              "Gate: every member in the source, attributes match, every link valid (one pass)", keep_id=True),
        **gate("Day_Lineage", "$.lineageq.qid", "$.lineageres", "Day_Passed", "Day_LineageFailed"),
        "Day_LineageFailed": fail("DayLineageCheckFailed",
                                  "States.Format('{} {} lineage gate failed: {}', $.dataset, $.day, States.JsonToString($.lineageres.row))"),
        "Day_Passed": {"Type": "Succeed"},
    }
    states = {
        "BQ2_Plan": control({"action": "plan"}, "$.bq2.plan", "BQ2_SourceBefore",
                            "Role 5: find the live copy (A or B) and plan the build into the other one"),
        "BQ2_SourceBefore": athena(f"{P}.source_sql_before", "$.bq2.srcq", "BQ2_SourceBefore_Result",
                                   "Gate: transactions_clean complete (exact rows, unique ids, every day) and fingerprint its files",
                                   keep_id=True),
        **gate("BQ2_SourceBefore", "$.bq2.srcq.qid", "$.bq2.src_before", "BQ2_ResetTarget",
               "BQ2_SourceIncomplete", {"fingerprint": 2}),
        "BQ2_SourceIncomplete": fail("SourceIncomplete", "$.bq2.src_before.row[1].VarCharValue"),
        "BQ2_ResetTarget": control({"action": "reset", "target.$": f"{P}.target"}, None, "BQ2_CreateTable",
                                   "Drop and empty the copy being built (refuses if it is live)", selector=False),
        "BQ2_CreateTable": athena(f"{P}.create_sql", None, "BQ2_BuildDays", "Create the empty copy (v1 schema)"),
        "BQ2_BuildDays": {
            "Type": "Map",
            "Comment": "Build and gate each dataset-day, 5 at a time. Any failed day stops the run before publish",
            "ItemsPath": f"{P}.days",
            "ItemSelector": {"dataset.$": "$$.Map.Item.Value.dataset", "day.$": "$$.Map.Item.Value.day",
                             "target_table.$": f"{P}.target_table"},
            "MaxConcurrency": 5,
            "ItemProcessor": {"ProcessorConfig": {"Mode": "INLINE"}, "StartAt": "Day_Render", "States": day_states},
            "ResultPath": None,
            "Next": "BQ2_Structure"},
        "BQ2_Structure": athena(f"{P}.structure_sql", "$.bq2.structq", "BQ2_Structure_Result",
                                "Gate: every derived column of every row recomputed", keep_id=True),
        **gate("BQ2_Structure", "$.bq2.structq.qid", "$.bq2.struct", "BQ2_SourceAfter", "BQ2_StructureFailed"),
        "BQ2_StructureFailed": fail("StructureCheckFailed", "States.JsonToString($.bq2.struct.row)"),
        "BQ2_SourceAfter": athena(f"{P}.source_sql_after", "$.bq2.srcq", "BQ2_SourceAfter_Result",
                                  "Gate: source still complete and unchanged during the build", keep_id=True),
        "BQ2_SourceAfter_Result": {"Type": "Task", "Resource": "arn:aws:states:::athena:getQueryResults",
                                   "Parameters": {"QueryExecutionId.$": "$.bq2.srcq.qid"},
                                   "ResultSelector": {"ok.$": "$.ResultSet.Rows[1].Data[0].VarCharValue",
                                                      "fingerprint.$": "$.ResultSet.Rows[1].Data[2].VarCharValue",
                                                      "row.$": "$.ResultSet.Rows[1].Data"},
                                   "ResultPath": "$.bq2.src_after", "Next": "BQ2_SourceUnchanged?"},
        "BQ2_SourceUnchanged?": {"Type": "Choice", "Choices": [{"And": [
            {"Variable": "$.bq2.src_after.ok", "StringEquals": "true"},
            {"Variable": "$.bq2.src_after.fingerprint", "StringEqualsPath": "$.bq2.src_before.fingerprint"}],
            "Next": "BQ2_Publish"}], "Default": "BQ2_SourceChanged"},
        "BQ2_SourceChanged": fail("SourceChangedDuringBuild",
                                  "States.Format('transactions_clean changed during the build. Before: {} After: {}', "
                                  "$.bq2.src_before.fingerprint, States.JsonToString($.bq2.src_after.row))"),
        "BQ2_Publish": athena(f"{P}.publish_sql", None, "BQ2_Done",
                              "Switch the bq2_sequences view to the new copy (the old copy stays as rollback)"),
        "BQ2_Done": {"Type": "Pass", "Comment": "Keep only a small summary for the rest of the pipeline",
                     "Parameters": {"live.$": f"{P}.target_table", "previous.$": f"{P}.live"},
                     "ResultPath": "$.bq2", "Next": "BQ2_Lift_Reset"},
    }
    return states


def sql_statements(path):
    """Statements of a Role 6 file, comments removed, DROPs skipped (the reset Lambda does that)."""
    body = "\n".join(l for l in path.read_text().splitlines() if not l.lstrip().startswith("--"))
    out = [x.strip() for x in body.split(";") if x.strip()]
    return [x for x in out if not x.upper().startswith("DROP")]


def lift_queries():
    one = lambda f: sql_statements(LIFT / f)
    stage1 = one("recovery/bq2_tradeoff_two_stage.sql")[:4]
    assert stage1[0].startswith("CREATE TABLE is459_g2t1_aml.bq2_tradeoff_flagged")
    assert all(x.startswith("INSERT INTO is459_g2t1_aml.bq2_tradeoff_flagged") for x in stage1[1:])
    q = [("Lift", "lift table", one("bq2_lift.sql")),
         ("Flagged_HI2", "trade-off stage 1: HI pairs (create)", [stage1[0]]),
         ("Flagged_HI3", "trade-off stage 1: HI triples", [stage1[1]]),
         ("Flagged_LI2", "trade-off stage 1: LI pairs", [stage1[2]]),
         ("Flagged_LI3", "trade-off stage 1: LI triples", [stage1[3]]),
         ("Tradeoff", "trade-off stage 2", one("recovery/tradeoff_stage2.sql")),
         ("Robustness", "LI robustness", one("robustness.sql")),
         ("Sankey", "Sankey", one("sankey.sql"))]
    for name, _, stmts in q:
        assert len(stmts) == 1, (name, len(stmts))
    return [(n, c, f"-- bq2 lift {n.lower()}\n" + st[0]) for n, c, st in q]


def lift_states(end_next=None):
    """Role 6 chain. Rerunnable: the reset drops all five tables first, so the INSERTs never append to old rows."""
    def literal(sql, nxt, comment, result_path=None, keep_id=False):
        st = athena("$", result_path, nxt, comment, keep_id)
        st["Parameters"] = {"QueryString": sql, **ATHENA}
        return st
    queries = lift_queries()
    names = [f"BQ2_{n}" for n, _, _ in queries]
    states = {"BQ2_Lift_Reset": {
        "Type": "Task", "Comment": "Role 6: drop the 5 Role 6 tables and empty their folders so the chain can rerun",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {"FunctionName": RESET, "Payload": {
            "bucket": "is459-g2t1-aml-processed-bucket", "database": "is459_g2t1_aml",
            "outputs": [{"table": t, "prefix": f"{t}/"} for t in LIFT_TABLES]}},
        "Retry": LAMBDA_RETRY, "ResultPath": None, "Next": names[0]}}
    for i, (n, comment, sql) in enumerate(queries):
        nxt = names[i + 1] if i + 1 < len(names) else "BQ2_Lift_Check"
        states[names[i]] = literal(sql, nxt, f"Role 6: {comment}")
    gate_sql = (LIFT / "pipeline_gate.sql").read_text().strip().rstrip(";")
    states["BQ2_Lift_Check"] = literal(gate_sql, "BQ2_Lift_Check_Result",
                                       "Gate: lift, trade-off, robustness and Sankey reconcile with bq2_sequences",
                                       "$.lift.checkq", keep_id=True)
    states.update(gate("BQ2_Lift_Check", "$.lift.checkq.qid", "$.lift.check", "BQ2_Lift_Done", "BQ2_LiftCheckFailed"))
    states["BQ2_LiftCheckFailed"] = fail("LiftCheckFailed", "States.JsonToString($.lift.check.row)")
    states["BQ2_Lift_Done"] = {"Type": "Pass", "Comment": "Role 6 outputs rebuilt and checked",
                               "Parameters": {"lift_check.$": "$.lift.check.ok"}, "ResultPath": "$.lift",
                               **({"Next": end_next} if end_next else {"End": True})}
    return states


def lock_state(nxt):
    return {"Type": "Task", "Comment": "Role 1: one pipeline execution at a time (fails fast if an earlier one is running)",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {"FunctionName": CONTROL, "Payload": {"action": "lock", "execution_arn.$": "$$.Execution.Id",
                                                                "state_machine_arn.$": "$$.StateMachine.Id"}},
            "Retry": LAMBDA_RETRY, "ResultPath": None, "Next": nxt}


def main():
    d = json.loads(ASL.read_text())
    d["Comment"] = ("IS459 G2T1 AML pipeline: lock, ingest external rates, crawl, ETL, then BQ1 corridors, "
                    "BQ1 graph and BQ2 (gated sequences, then lift) in parallel, then the BQ1 priority list.")
    if d["StartAt"] != "PipelineLock":
        d["States"] = {"PipelineLock": lock_state(d["StartAt"]), **d["States"]}
        d["StartAt"] = "PipelineLock"
    branch = next(b for b in d["States"]["Analytics"]["Branches"]
                  if "BQ2_LiftAnalysis" in b["States"] or "BQ2_Lift_Reset" in b["States"])
    branch["States"] = {**bq2_states(), **lift_states()}
    branch["StartAt"] = "BQ2_Plan"
    text = json.dumps(d, indent=2)
    ASL.write_text(text + "\n")
    print(f"wrote {ASL.name}: {len(text):,} characters")
    if "--lift-only" in sys.argv:
        small = {"Comment": "Role 6 chain only, for testing before it runs inside is459-g2t1-pipeline",
                 "StartAt": "BQ2_Lift_Reset", "States": lift_states()}
        out = LIFT / "lift_only.asl.json"
        out.write_text(json.dumps(small, indent=2) + "\n")
        print(f"wrote {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
