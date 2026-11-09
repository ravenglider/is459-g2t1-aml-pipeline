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
"""
import json
from pathlib import Path

ASL = Path(__file__).resolve().parents[3] / "ingestion/stepfunctions/is459-g2t1-pipeline.asl.json"
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
                     "ResultPath": "$.bq2", "Next": "BQ2_LiftAnalysis"},
    }
    return states


def lock_state(nxt):
    return {"Type": "Task", "Comment": "Role 1: one pipeline execution at a time (fails fast if an earlier one is running)",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {"FunctionName": CONTROL, "Payload": {"action": "lock", "execution_arn.$": "$$.Execution.Id",
                                                                "state_machine_arn.$": "$$.StateMachine.Id"}},
            "Retry": LAMBDA_RETRY, "ResultPath": None, "Next": nxt}


def main():
    d = json.loads(ASL.read_text())
    if d["StartAt"] != "PipelineLock":
        d["States"] = {"PipelineLock": lock_state(d["StartAt"]), **d["States"]}
        d["StartAt"] = "PipelineLock"
    branch = next(b for b in d["States"]["Analytics"]["Branches"] if "BQ2_LiftAnalysis" in b["States"])
    lift = branch["States"]["BQ2_LiftAnalysis"]
    branch["States"] = {**bq2_states(), "BQ2_LiftAnalysis": lift}
    branch["StartAt"] = "BQ2_Plan"
    text = json.dumps(d, indent=2)
    ASL.write_text(text + "\n")
    print(f"wrote {ASL.name}: {len(text):,} characters")


if __name__ == "__main__":
    main()
