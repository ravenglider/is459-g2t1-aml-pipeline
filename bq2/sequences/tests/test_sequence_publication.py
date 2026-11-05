"""Increment 3A tests: supplied snapshots only, no Spark, AWS, credentials or IO."""

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import sequence_publication as pub
from sequence_manifest import create_run_manifest, attach_validated_attempt, transition_manifest, _digest


class PublicationPlanningTests(unittest.TestCase):
    def setUp(self):
        self.dates = ["2022-09-01", "2022-09-02", "2022-09-03"]
        self.previous = dict(run_id="previous", manifest_fingerprint="b" * 64)
        columns = [dict(Name=name, Type=kind) for name, kind in pub.DATA_COLUMNS]
        self.table = dict(DatabaseName=pub.DATABASE, Name=pub.TABLE,
            StorageDescriptor=dict(Location=pub.TABLE_ROOT, Columns=columns,
                InputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                OutputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                SerdeInfo=dict(SerializationLibrary="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")),
            PartitionKeys=[dict(Name="dataset", Type="string"), dict(Name="start_date", Type="string")],
            Parameters=dict(classification="parquet"))
        self.partitions = [self.partition("sample", "2022-09-01"), self.partition("sample", "2022-09-02"),
                           self.partition("sample", "2022-09-04"), self.partition("LI-Medium", "2022-09-01")]
        self.evidence = {}
        for index, day in enumerate(self.dates):
            uri = pub.STAGING_ROOT + "run/attempt_" + str(index) + "/"
            counts = {"2": 2 if index == 0 else 0, "3": 0}
            receipt = dict(run_id="run", attempt_path=uri, counts=counts,
                fingerprints=dict(status="c" * 64, validation="d" * 64, inventory="e" * 64),
                status_receipt=uri + "metadata/status.json", validation_receipt=uri + "metadata/validation.json",
                status=dict(format_version=1, state="VALIDATED", dataset="sample", start_date=day,
                            timezone="UTC", source_fingerprint="a" * 64, expected_counts=counts))
            self.evidence[day] = dict(receipt=receipt, verified=True,
                verification_reference="Synthetic Parquet/inventory check only", inventory_fingerprint="e" * 64)
        self.recovery = {}
        for definition in self.partitions[:2]:
            self.recovery[definition["Values"][1]] = dict(verified=True, partition_fingerprint=_digest(definition),
                inventory_fingerprint="f" * 64, counts={"2": 3, "3": 1}, publication_reference=self.previous,
                verification_reference="Synthetic old data recovery evidence")
        self.run = create_run_manifest("run", "development", "sample", "partial", self.dates,
            builder=dict(version="synthetic", dirty=False, code_fingerprint="1" * 64),
            timezone="UTC", timezone_evidence="Synthetic upstream timezone check",
            source_coverage=dict(start_date="2022-09-01", end_date="2022-09-04", verified=True,
                                 evidence="Synthetic complete source extract"),
            source_inventory=dict(fingerprint="a" * 64, evidence="Synthetic inventory"),
            identity_check=dict(dataset="sample", identity_check_passed=True, row_count=20,
                                verification_scope="provided_dataframe_only", dataset_wide_verified=False),
            coverage_evidence=dict(dataset="sample", verified=True, checked_dates=self.dates + ["2022-09-04"],
                source_inventory_fingerprint="a" * 64, evidence="Synthetic full source scan"),
            previous_publication=self.previous, expected_current_publication_fingerprint="2" * 64)
        for day in self.dates:
            self.run = attach_validated_attempt(self.run, self.evidence[day]["receipt"]["attempt_path"], receipt_loader=self.loader)
        self.run = transition_manifest(self.run, "READY_TO_PLAN", current_source_fingerprint="a" * 64, receipt_loader=self.loader)
        self.options = dict(frozen_run_fingerprint=_digest(self.run), table=self.table, partitions=self.partitions,
            staging_evidence=self.evidence, recovery_evidence=self.recovery, current_publication=self.previous,
            current_source_fingerprint="a" * 64,
            owner=dict(table=pub.DATABASE + "." + pub.TABLE, token="owner", evidence_reference="Synthetic owner attestation"),
            maintenance_ack=dict(table=pub.DATABASE + "." + pub.TABLE, publication_id="publication_1",
                acknowledged=True, active_queries_drained=True, acknowledged_by="Xiang Ying",
                reference="Synthetic maintenance window; no actual queries checked"))

    def partition(self, dataset, day):
        storage = deepcopy(self.table["StorageDescriptor"])
        storage["Location"] = pub.TABLE_ROOT + f"dataset={dataset}/start_date={day}/"
        return dict(Values=[dataset, day], StorageDescriptor=storage, Parameters=dict(old="preserve"))

    def loader(self, path):
        return next(e["receipt"] for e in self.evidence.values() if e["receipt"]["attempt_path"] == path)

    def plan(self, run=None, dates=None, publication_id="publication_1", **changes):
        return pub.plan_development_publication(self.run if run is None else run, publication_id,
            self.dates if dates is None else dates, **dict(self.options, **changes))

    def test_sample_development_only_and_partial_scope(self):
        for changes in (dict(database="wrong"), dict(table_name="bq2_sequences"), dict(bucket="other"),
                        dict(destination="production"), dict(dataset="HI-Medium"), dict(dataset="LI-Medium"), dict(scope="full")):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.plan(**changes)
        for changes in (dict(dataset="HI-Medium"), dict(destination="production")):
            changed = dict(self.run, **changes)
            with self.assertRaises(ValueError):
                self.plan(changed, frozen_run_fingerprint=_digest(changed))

    def test_replace_remove_empty_and_preserve_unrelated_state(self):
        plan = self.plan()
        actions = plan["contents"]["actions"]
        self.assertEqual([a["outcome"] for a in actions], ["REPLACE", "REMOVE", "EMPTY"])
        self.assertEqual([a["catalog_action"] for a in actions], ["UPDATE", "DELETE", "NONE"])
        self.assertEqual(actions[0]["expected_old"], self.partitions[0])
        self.assertEqual(actions[0]["intended_new"]["StorageDescriptor"]["Location"],
                         pub.VERSION_ROOT + "publication_1/dataset=sample/start_date=2022-09-01/")
        self.assertEqual(actions[0]["intended_new"]["StorageDescriptor"]["Columns"], self.table["StorageDescriptor"]["Columns"])
        self.assertIsNone(actions[1]["intended_new"])
        self.assertEqual(plan["contents"]["preserved_partitions"], sorted(self.partitions[2:], key=lambda p: p["Values"]))
        self.assertEqual(actions[0]["recovery"], self.recovery[self.dates[0]])
        self.assertFalse(plan["contents"]["controls"]["atomic_multi_partition"])

    def test_subset_of_run_never_removes_unselected_dates(self):
        plan = self.plan(dates=[self.dates[0]])
        self.assertEqual(len(plan["contents"]["actions"]), 1)
        self.assertEqual(len(plan["contents"]["preserved_partitions"]), 3)
        # A full build can still feed a partial publication: scope is separate.
        full = deepcopy(self.run)
        full["refresh_scope"] = "full"
        full["source_coverage"]["end_date"] = "2022-09-03"
        full["coverage_evidence"]["checked_dates"] = self.dates
        self.plan(full, dates=[self.dates[0]], frozen_run_fingerprint=_digest(full))

    def test_new_partition_uses_create_and_versioned_old_locations_are_supported(self):
        self.assertEqual(self.plan(partitions=self.partitions[1:])["contents"]["actions"][0]["catalog_action"], "CREATE")
        old = deepcopy(self.partitions)
        old[0]["StorageDescriptor"]["Location"] = pub.VERSION_ROOT + "previous/dataset=sample/start_date=2022-09-01/"
        recovery = deepcopy(self.recovery)
        recovery[self.dates[0]]["partition_fingerprint"] = _digest(old[0])
        plan = self.plan(partitions=old, recovery_evidence=recovery)
        self.assertEqual(plan["contents"]["actions"][0]["expected_old"], old[0])

    def test_deterministic_order_fingerprint_and_inputs_unchanged(self):
        snapshot = deepcopy((self.run, self.options))
        plan = self.plan()
        self.assertEqual(plan, self.plan(dates=list(reversed(self.dates)), partitions=list(reversed(self.partitions))))
        self.assertEqual((self.run, self.options), snapshot)
        self.assertEqual(plan["fingerprint"], _digest(plan["contents"]))
        self.assertEqual([a["operation_id"] for a in plan["contents"]["actions"]], ["op-0001", "op-0002", "op-0003"])

    def test_stale_table_or_partition_snapshot_invalidates_plan(self):
        plan = self.plan()
        pub.assert_publication_plan_current(plan, self.run, self.dates, **self.options)
        table = deepcopy(self.table)
        table["Parameters"]["new_parameter"] = "changed"
        partitions = deepcopy(self.partitions)
        partitions[-1]["Parameters"]["old"] = "changed unrelated partition"
        for changes in (dict(table=table), dict(partitions=partitions)):
            with self.assertRaisesRegex(ValueError, "Stale"):
                pub.assert_publication_plan_current(plan, self.run, self.dates, **dict(self.options, **changes))

    def test_changed_staging_receipt_and_frozen_run_are_rejected(self):
        changed = deepcopy(self.run)
        changed["builder"]["version"] = "new code"
        with self.assertRaisesRegex(ValueError, "Frozen"):
            self.plan(changed)
        evidence = deepcopy(self.evidence)
        evidence[self.dates[0]]["receipt"]["fingerprints"]["validation"] = "9" * 64
        with self.assertRaisesRegex(ValueError, "evidence changed"):
            self.plan(staging_evidence=evidence)
        changed = deepcopy(self.evidence)
        changed[self.dates[0]]["inventory_fingerprint"] = "9" * 64
        with self.assertRaisesRegex(ValueError, "Stale"):
            pub.assert_publication_plan_current(self.plan(), self.run, self.dates,
                **dict(self.options, staging_evidence=changed))

    def test_missing_or_mismatched_recovery_evidence_blocks_replace_and_remove(self):
        for day in self.dates[:2]:
            for field, value in (("verified", False), ("inventory_fingerprint", ""),
                                  ("partition_fingerprint", "9" * 64), ("publication_reference", None),
                                  ("verification_reference", ""), ("counts", {})):
                recovery = deepcopy(self.recovery)
                recovery[day][field] = value
                with self.subTest(day=day, field=field), self.assertRaises(ValueError):
                    self.plan(recovery_evidence=recovery)
        with self.assertRaises(ValueError):
            self.plan(recovery_evidence={})
        with self.assertRaises(ValueError):
            self.plan(current_publication=None)

    def test_run_state_eligibility_and_local_planned_validation(self):
        for state in ("CREATED", "STAGING", "FAILED", "PUBLISHING", "PUBLISHED"):
            changed = dict(self.run, state=state)
            with self.subTest(state=state), self.assertRaises(ValueError):
                self.plan(changed, frozen_run_fingerprint=_digest(changed))
        local = deepcopy(self.run)
        local["state"] = "PLANNED"
        with self.assertRaisesRegex(ValueError, "complete"):
            self.plan(local, frozen_run_fingerprint=_digest(local))
        # Synthetic legacy fixed-location plan checked by shared completeness rules.
        contents = {key: local[key] for key in ("run_id", "destination", "dataset", "refresh_scope",
                                               "expected_current_publication_fingerprint")}
        contents["actions"] = [dict(dataset="sample", start_date=day, counts=local["batches"][day]["counts"],
            action="REPLACE" if i == 0 else ("REMOVE" if i == 1 else "EMPTY"),
            previous_partition=self.partitions[i] if i < 2 else None) for i, day in enumerate(self.dates)]
        local["plan"] = dict(contents=contents, fingerprint=_digest(contents))
        planned = self.plan(local, frozen_run_fingerprint=_digest(local))
        self.assertEqual(planned["contents"]["run_reference"]["state"], "PLANNED")
        self.assertNotEqual(planned["contents"]["actions"], local["plan"]["contents"]["actions"])

    def test_source_completeness_and_validated_staging_evidence_required(self):
        with self.assertRaisesRegex(ValueError, "Source inventory changed"):
            self.plan(current_source_fingerprint="9" * 64)
        for change in ("verified", "state", "coverage"):
            evidence = deepcopy(self.evidence)
            if change == "verified":
                evidence[self.dates[0]]["verified"] = False
            elif change == "state":
                evidence[self.dates[0]]["receipt"]["status"]["state"] = "WRITING"
            else:
                del evidence[self.dates[0]]
            with self.assertRaises(ValueError):
                self.plan(staging_evidence=evidence)
        incomplete = deepcopy(self.run)
        incomplete["coverage_evidence"]["checked_dates"] = [self.dates[0]]
        with self.assertRaisesRegex(ValueError, "dataset-wide"):
            self.plan(incomplete, frozen_run_fingerprint=_digest(incomplete))
        self.assertFalse(self.run["identity_check"]["dataset_wide_verified"])

    def test_table_wide_owner_and_athena_acknowledgment_are_explicit(self):
        for changes in (dict(owner={}), dict(owner=dict(self.options["owner"], table="other")),
                        dict(maintenance_ack={}), dict(maintenance_ack=dict(self.options["maintenance_ack"], active_queries_drained=False)),
                        dict(maintenance_ack=dict(self.options["maintenance_ack"], publication_id="other"))):
            with self.assertRaises(ValueError):
                self.plan(**changes)

    def test_invalid_paths_dates_ids_duplicate_partitions_and_schema(self):
        for identifier in ("", "../x", "x/y", "x%2Fy", "x" * 65):
            with self.assertRaises(ValueError):
                self.plan(publication_id=identifier)
        for dates in ([], ["20220901"], ["2022-02-30"], ["2022-09-04"], [self.dates[0]] * 2):
            with self.assertRaises(ValueError):
                self.plan(dates=dates)
        for location in ("s3://other/path/", pub.ROOT + "bq2_sequences/dataset=sample/start_date=2022-09-01/",
                         pub.VERSION_ROOT + "../dataset=sample/start_date=2022-09-01/",
                         pub.VERSION_ROOT + "x%2Fy/dataset=sample/start_date=2022-09-01/"):
            bad = deepcopy(self.partitions)
            bad[0]["StorageDescriptor"]["Location"] = location
            with self.assertRaises(ValueError):
                self.plan(partitions=bad)
        with self.assertRaises(ValueError):
            self.plan(partitions=self.partitions + [self.partitions[0]])
        bad = deepcopy(self.table)
        bad["StorageDescriptor"]["Columns"][0]["Type"] = "bigint"
        with self.assertRaises(ValueError):
            self.plan(table=bad)

    def test_local_staging_is_offline_only_and_records_execution_blockers(self):
        local, evidence = deepcopy(self.run), deepcopy(self.evidence)
        for index, day in enumerate(self.dates):
            path = "/synthetic/staging/run/attempt_" + str(index)
            receipt = evidence[day]["receipt"]
            receipt["attempt_path"] = path
            receipt["status_receipt"] = path + "/metadata/status.json"
            receipt["validation_receipt"] = path + "/metadata/validation.json"
            batch = local["batches"][day]
            for key in ("attempt_path", "status_receipt", "validation_receipt"):
                batch[key] = receipt[key]
        plan = self.plan(local, staging_evidence=evidence, frozen_run_fingerprint=_digest(local))
        self.assertEqual(len(plan["contents"]["execution_blockers"]), 3)

    def test_ordered_json_journal_intents_observations_and_no_success_claim(self):
        journal = pub.create_publication_manifest(self.plan())
        original = deepcopy(journal)
        with self.assertRaises(ValueError):
            pub.record_operation_intent(journal, "op-0002")
        with self.assertRaises(ValueError):
            pub.record_operation_observation(journal, "op-0001", None, reference="synthetic")
        first = pub.record_operation_intent(journal, "op-0001")
        self.assertEqual(journal, original)
        self.assertEqual(first["journal"][0]["status"], "INTENT_RECORDED")
        old = first["journal"][0]["intent"]["expected_old"]
        observed = pub.record_operation_observation(first, "op-0001", old, reference="synthetic old read")
        self.assertEqual(observed["journal"][0]["status"], "OBSERVED_OLD_SIMULATED")
        for entry in list(observed["journal"]):
            op = entry["operation_id"]
            if op != "op-0001":
                observed = pub.record_operation_intent(observed, op)
            observed = pub.record_operation_observation(observed, op, entry["intent"]["intended_new"], reference="synthetic new read")
        self.assertEqual(observed["state"], "SIMULATING")
        self.assertFalse(observed["publication_success_allowed"])
        self.assertEqual(json.loads(json.dumps(observed)), observed)
        for state in ("PREPARING", "PREPARED", "PUBLISHING", "VERIFYING", "PUBLISHED", "ROLLED_BACK"):
            with self.assertRaises(ValueError):
                pub.transition_publication_manifest(observed, state)

    def test_interrupted_operation_classification_and_conflict_recovery(self):
        actions = self.plan()["contents"]["actions"]
        for action in actions[:2]:
            self.assertEqual(pub.classify_operation_state(action, action["expected_old"]), "EXPECTED_OLD")
            self.assertEqual(pub.classify_operation_state(action, action["intended_new"]), "INTENDED_NEW")
        self.assertEqual(pub.classify_operation_state(actions[2], None), "ALREADY_EMPTY")
        conflicting = deepcopy(actions[0]["expected_old"])
        conflicting["Parameters"]["unexpected"] = "concurrent change"
        self.assertEqual(pub.classify_operation_state(actions[0], conflicting), "CONFLICT")
        journal = pub.record_operation_intent(pub.create_publication_manifest(self.plan()), "op-0001")
        journal = pub.record_operation_observation(journal, "op-0001", conflicting, reference="synthetic conflict")
        self.assertEqual(journal["state"], "RECOVERY_REQUIRED")
        with self.assertRaises(ValueError):
            pub.record_operation_intent(journal, "op-0002")

    def test_journal_identity_intent_and_plan_tampering_are_rejected(self):
        journal = pub.create_publication_manifest(self.plan())
        bad = deepcopy(journal)
        bad["journal"][0]["intent"]["catalog_action"] = "DELETE"
        with self.assertRaises(ValueError):
            pub.record_operation_intent(bad, "op-0001")
        bad = deepcopy(journal)
        bad["plan"]["contents"]["table"] = "bq2_sequences"
        with self.assertRaises(ValueError):
            pub.record_operation_intent(bad, "op-0001")
        self.assertEqual(pub.transition_publication_manifest(journal, "RECOVERY_REQUIRED")["state"], "RECOVERY_REQUIRED")

    def test_planning_performs_no_network_or_filesystem_operations(self):
        with patch("socket.socket", side_effect=AssertionError("network forbidden")), patch(
                "builtins.open", side_effect=AssertionError("file IO forbidden")):
            self.plan()
        script = '''
import sys, importlib.abc
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'boto3','botocore','awsglue','pyspark'}:
            raise AssertionError('Forbidden dependency: ' + fullname)
sys.meta_path.insert(0, Block())
sys.path.insert(0, sys.argv[1])
import sequence_publication
'''
        result = subprocess.run([sys.executable, "-I", "-c", script, str(Path(__file__).resolve().parents[1])],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
