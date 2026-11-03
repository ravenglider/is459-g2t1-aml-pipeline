"""Pure-Python manifest tests with synthetic receipts; no Spark, Parquet or AWS.

Receipts in these fixtures simulate the validated staging handoff. They do not
claim to certify actual transaction files or real source coverage.
"""

from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sequence_manifest import (
    DESTINATIONS, create_run_manifest, attach_validated_attempt, validate_run_readiness,
    plan_partition_replacement, transition_manifest, write_manifest_local,
    publication_lock_local, _publication_fingerprint, _receipt_evidence,
)


class ManifestTests(unittest.TestCase):
    def test_injected_receipt_loader_preserves_local_attachment_and_readiness_rules(self):
        original = self.manifest()
        paths = [self.attempt(day, name="injected-" + day) for day in self.dates]
        loader = lambda path: _receipt_evidence(path)
        with patch("sequence_manifest._now", return_value="2026-01-01T00:00:00.000000Z"):
            local, injected = original, original
            for path in paths:
                local = attach_validated_attempt(local, path)
                injected = attach_validated_attempt(injected, path, receipt_loader=loader)
            self.assertEqual(local, injected)
            self.assertEqual(transition_manifest(local, "READY_TO_PLAN", current_source_fingerprint=self.source_fingerprint),
                transition_manifest(injected, "READY_TO_PLAN", current_source_fingerprint=self.source_fingerprint,
                                    receipt_loader=loader))
        bad = dict(loader(paths[0]), run_id="another-run")
        with self.assertRaisesRegex(ValueError, "ownership"):
            attach_validated_attempt(original, paths[0], receipt_loader=lambda path: bad)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="role5-manifest-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source_fingerprint = "a" * 64
        self.previous = dict(run_id="previous", manifest_fingerprint="b" * 64)
        self.dates = ["2022-09-01", "2022-09-02", "2022-09-03"]
        self.inventory = [self.partition("sample", "2022-09-01"),
                          self.partition("sample", "2022-09-02"),
                          self.partition("sample", "2022-09-04"),
                          self.partition("LI-Medium", "2022-09-02")]

    def partition(self, dataset, day, destination="development"):
        return dict(dataset=dataset, start_date=day,
            location=f"{DESTINATIONS[destination][1]}dataset={dataset}/start_date={day}/",
            counts={"2": 2, "3": 1}, publication_run_id="previous")

    def manifest(self, **changes):
        options = dict(run_id="run", destination="development", dataset="sample", refresh_scope="full",
            start_dates=self.dates, builder=dict(version="c" * 40, dirty=False, code_fingerprint="d" * 64),
            timezone="UTC", timezone_evidence="synthetic UTC test setting, not production evidence",
            source_coverage=dict(start_date=self.dates[0], end_date=self.dates[-1],
                                 verified=True, evidence="synthetic complete inventory"),
            source_inventory=dict(fingerprint=self.source_fingerprint, evidence="synthetic inventory record"),
            identity_check=dict(dataset="sample", row_count=10, identity_check_passed=True,
                                verification_scope="provided_dataframe_only", dataset_wide_verified=False),
            coverage_evidence=dict(verified=True, dataset="sample", checked_dates=self.dates,
                source_inventory_fingerprint=self.source_fingerprint, evidence="synthetic full identity-scan coverage"),
            previous_publication=self.previous,
            expected_current_publication_fingerprint=_publication_fingerprint(self.inventory, "development", self.previous))
        options.update(changes)
        return create_run_manifest(**options)

    def attempt(self, day, counts=None, name=None, **changes):
        counts = {"2": 2, "3": 1} if counts is None else counts
        path = self.root / "staging" / "run" / (name or f"attempt-{day}")
        (path / "metadata").mkdir(parents=True)
        (path / "data").mkdir()
        status = dict(format_version=1, state="VALIDATED", dataset="sample", start_date=day,
                      timezone="UTC", expected_counts=counts)
        status.update(changes)
        report = dict(counts=counts, row_count=sum(counts.values()), schema_valid=True, values_valid=True)
        (path / "metadata" / "status.json").write_text(json.dumps(status), encoding="utf-8")
        (path / "metadata" / "validation.json").write_text(json.dumps(report), encoding="utf-8")
        return path

    def staged(self, manifest=None):
        result = self.manifest() if manifest is None else manifest
        for index, day in enumerate(result["start_dates"]):
            result = attach_validated_attempt(result, self.attempt(day,
                {"2": 2, "3": 1} if index == 0 else {"2": 0, "3": 0}))
        return result

    def ready(self, manifest=None):
        return transition_manifest(self.staged(manifest), "READY_TO_PLAN",
                                   current_source_fingerprint=self.source_fingerprint)

    def plan(self, manifest, inventory=None, previous=None):
        return plan_partition_replacement(manifest, self.inventory if inventory is None else inventory,
            current_publication=self.previous if previous is None else previous,
            current_source_fingerprint=self.source_fingerprint)

    def test_full_and_partial_scope_and_builder_evidence(self):
        full = self.manifest()
        self.assertEqual(full["start_dates"], self.dates)
        self.assertFalse(full["builder"]["dirty"])
        dirty = self.manifest(builder=dict(version="working-tree", dirty=True, code_fingerprint="f" * 64))
        self.assertTrue(dirty["builder"]["dirty"])
        with self.assertRaises(ValueError):
            self.manifest(start_dates=self.dates[:1])
        with self.assertRaises(ValueError):
            self.manifest(start_dates=[self.dates[0], self.dates[0]])
        with self.assertRaises(ValueError):
            self.manifest(builder=dict(version="commit", dirty=True))
        partial = self.manifest(refresh_scope="partial", start_dates=self.dates[:1])
        self.assertTrue(partial["mixed_source_versions_possible"])

    def test_missing_and_duplicate_attempts(self):
        manifest = self.manifest()
        path = self.attempt(self.dates[0])
        staged = attach_validated_attempt(manifest, path)
        self.assertEqual(manifest["batches"], {})  # APIs return new records.
        with self.assertRaisesRegex(ValueError, "Missing selected"):
            validate_run_readiness(staged, current_source_fingerprint=self.source_fingerprint)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            attach_validated_attempt(staged, path)
        with self.assertRaises(ValueError):
            attach_validated_attempt(manifest, self.root / "missing")

    def test_nonvalidated_ownership_and_count_mismatches(self):
        for index, changes in enumerate((dict(state="WRITING"), dict(state="VALIDATING"), dict(state="FAILED"),
                                        dict(dataset="HI-Medium"), dict(timezone="Asia/Singapore"),
                                        dict(start_date="2022-09-04"), dict(expected_counts={"2": 9, "3": 1}))):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                attach_validated_attempt(self.manifest(), self.attempt(self.dates[0], name=f"bad-{index}", **changes))
        path = self.attempt(self.dates[0], name="incomplete")
        (path / "metadata" / "validation.json").unlink()
        with self.assertRaisesRegex(ValueError, "receipts"):
            attach_validated_attempt(self.manifest(), path)

    def test_receipt_fingerprints_checked_at_attachment_and_readiness(self):
        path = self.attempt(self.dates[0])
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            attach_validated_attempt(self.manifest(), path,
                expected_receipt_fingerprints=dict(status="0" * 64, validation="0" * 64))
        manifest = self.manifest()
        manifest = attach_validated_attempt(manifest, path)
        for day in self.dates[1:]:
            manifest = attach_validated_attempt(manifest, self.attempt(day, {"2": 0, "3": 0}))
        # Same JSON meaning, different bytes: original receipt evidence changed.
        receipt = path / "metadata" / "status.json"
        receipt.write_text(json.dumps(json.loads(receipt.read_text()), indent=2), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "evidence changed"):
            validate_run_readiness(manifest, current_source_fingerprint=self.source_fingerprint)

    def test_identity_report_is_not_promoted_to_global_verification(self):
        manifest = self.staged()
        result = validate_run_readiness(manifest, current_source_fingerprint=self.source_fingerprint)
        self.assertEqual(result["counts"], {"2": 2, "3": 1})
        self.assertFalse(manifest["identity_check"]["dataset_wide_verified"])
        for field, value in (("verified", False), ("checked_dates", self.dates[:1]),
                             ("source_inventory_fingerprint", "e" * 64), ("evidence", "")):
            broken = deepcopy(manifest)
            broken["coverage_evidence"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_run_readiness(broken, current_source_fingerprint=self.source_fingerprint)
        with self.assertRaisesRegex(ValueError, "Source inventory changed"):
            validate_run_readiness(manifest, current_source_fingerprint="e" * 64)
        corrupted = deepcopy(manifest)
        corrupted["batches"][self.dates[0]]["row_count"] = 0
        with self.assertRaises(ValueError):
            validate_run_readiness(corrupted, current_source_fingerprint=self.source_fingerprint)

    def test_full_replacement_zero_output_and_stale_removal(self):
        planned = self.plan(self.ready())
        self.assertEqual(planned["state"], "PLANNED")
        contents = planned["plan"]["contents"]
        self.assertEqual([(a["start_date"], a["action"]) for a in contents["actions"]],
                         [("2022-09-01", "REPLACE"), ("2022-09-02", "REMOVE"),
                          ("2022-09-03", "EMPTY"), ("2022-09-04", "REMOVE")])
        self.assertEqual(contents["actions"][-1]["reason"], "stale_outside_new_coverage")
        self.assertEqual(contents["preserved_partitions"], [self.inventory[-1]])
        self.assertEqual(contents["previous_publication"], self.previous)
        self.assertEqual(contents["actions"][0]["previous_partition"]["location"], self.inventory[0]["location"])
        self.assertIn("NOT backups", planned["requirements"]["recovery"])
        self.assertIn("Athena", planned["requirements"]["reader_coordination"])

    def test_partial_preserves_unselected_dates_and_other_datasets(self):
        manifest = self.manifest(refresh_scope="partial", start_dates=[self.dates[1]])
        manifest = attach_validated_attempt(manifest, self.attempt(self.dates[1], {"2": 0, "3": 0}))
        ready = transition_manifest(manifest, "READY_TO_PLAN", current_source_fingerprint=self.source_fingerprint)
        contents = self.plan(ready)["plan"]["contents"]
        self.assertEqual([(a["start_date"], a["action"]) for a in contents["actions"]], [(self.dates[1], "REMOVE")])
        self.assertEqual(len(contents["preserved_partitions"]), 3)
        self.assertIn(self.inventory[2], contents["preserved_partitions"])

    def test_partial_upstream_repair_requires_backward_dependencies_or_disclosure(self):
        with self.assertRaisesRegex(ValueError, "Partial source repair"):
            self.manifest(refresh_scope="partial", start_dates=[self.dates[-1]], upstream_changed_dates=[self.dates[-1]])
        with self.assertRaises(ValueError):
            self.manifest(refresh_scope="partial", start_dates=[self.dates[-1]],
                          upstream_changed_dates=[self.dates[-1]], repair_claim="exhaustive")
        bounded = self.manifest(refresh_scope="partial", start_dates=[self.dates[-1]],
                               upstream_changed_dates=[self.dates[-1]], repair_claim="bounded")
        self.assertEqual(bounded["repair"]["missing_start_dates"], self.dates[:2])
        self.assertFalse(bounded["repair"]["exhaustive"])
        complete = self.manifest(refresh_scope="partial", upstream_changed_dates=[self.dates[-1]], repair_claim="exhaustive")
        self.assertTrue(complete["repair"]["exhaustive"])

    def test_destination_isolation(self):
        with self.assertRaises(ValueError):
            self.manifest(destination="production")
        ready = self.ready()
        wrong = deepcopy(self.inventory)
        wrong[0]["location"] = wrong[0]["location"].replace("bq2_sequences_dev", "bq2_sequences")
        with self.assertRaises(ValueError):
            self.plan(ready, wrong)
        corrupted = deepcopy(ready)
        corrupted["destination"] = "production"
        with self.assertRaises(ValueError):
            self.plan(corrupted)

    def test_order_independent_fingerprint_and_idempotent_retry(self):
        ready = self.ready()
        first = self.plan(ready)
        second = self.plan(ready, list(reversed(self.inventory)))
        self.assertEqual(first["plan"], second["plan"])
        retry = self.plan(first, list(reversed(self.inventory)))
        self.assertEqual(retry, first)
        self.assertEqual(len(first["plan"]["contents"]["actions"]), 4)

    def test_stale_publication_and_duplicate_inventory_rejected(self):
        ready = self.ready()
        changed = deepcopy(self.inventory)
        changed[0]["publication_run_id"] = "another-run"
        with self.assertRaisesRegex(ValueError, "Stale"):
            self.plan(ready, changed)
        with self.assertRaisesRegex(ValueError, "Stale"):
            self.plan(ready, previous=dict(run_id="new", manifest_fingerprint="f" * 64))
        with self.assertRaisesRegex(ValueError, "Duplicate/conflicting"):
            self.plan(ready, self.inventory + [self.inventory[0]])
        with self.assertRaisesRegex(ValueError, "previous-publication"):
            _publication_fingerprint(self.inventory, "development", None)

    def test_first_publication_empty_inventory_requires_no_fake_previous_run(self):
        fingerprint = _publication_fingerprint([], "development", None)
        manifest = self.manifest(previous_publication=None, expected_current_publication_fingerprint=fingerprint)
        ready = self.ready(manifest)
        planned = plan_partition_replacement(ready, [], current_publication=None,
                                            current_source_fingerprint=self.source_fingerprint)
        self.assertIsNone(planned["previous_publication"])
        self.assertEqual([a["action"] for a in planned["plan"]["contents"]["actions"]],
                         ["REPLACE", "EMPTY", "EMPTY"])

    def test_lifecycle_forbids_publication_and_premature_planning(self):
        manifest = self.manifest()
        for state in ("READY_TO_PLAN", "PLANNED", "PUBLISHING", "PUBLISHED"):
            with self.subTest(state=state), self.assertRaises(ValueError):
                transition_manifest(manifest, state)
        ready = self.ready()
        with self.assertRaisesRegex(ValueError, "complete deterministic"):
            transition_manifest(ready, "PLANNED")
        malformed = deepcopy(ready)
        # Even a self-consistent hash cannot turn an incomplete plan into success.
        import hashlib
        malformed["plan"] = dict(contents={}, fingerprint=hashlib.sha256(b"{}").hexdigest())
        with self.assertRaisesRegex(ValueError, "complete deterministic"):
            transition_manifest(malformed, "PLANNED")
        failed = transition_manifest(ready, "FAILED", error="synthetic validation failure")
        self.assertEqual(failed["state"], "FAILED")
        self.assertIn("error", failed)
        with self.assertRaises(ValueError):
            transition_manifest(failed, "STAGING")
        broken = deepcopy(failed)
        broken["state"] = "PUBLISHED"
        with self.assertRaises(ValueError):
            write_manifest_local(broken, self.root / "metadata")

    def test_atomic_writes_interrupted_updates_and_revision_guards(self):
        manifest = self.manifest()
        path = write_manifest_local(manifest, self.root / "metadata")
        before = path.read_bytes()
        staged = transition_manifest(manifest, "STAGING")
        with patch("sequence_manifest.os.replace", side_effect=OSError("interrupted")):
            with self.assertRaisesRegex(OSError, "interrupted"):
                write_manifest_local(staged, self.root / "metadata", expected_revision=0)
        self.assertEqual(path.read_bytes(), before)
        self.assertTrue(list(path.parent.glob("*.tmp")))
        write_manifest_local(staged, self.root / "metadata", expected_revision=0)
        self.assertEqual(json.loads(path.read_text())["revision"], 1)
        with self.assertRaisesRegex(ValueError, "Stale"):
            write_manifest_local(staged, self.root / "metadata", expected_revision=0)
        with self.assertRaises(ValueError):
            write_manifest_local(staged, self.root / "bq2_sequences")

    def test_exclusive_lock_is_table_wide_and_released_on_error(self):
        root = self.root / "locks"
        with publication_lock_local(root, "development", "run") as owner:
            self.assertEqual(owner["table"], DESTINATIONS["development"][0])
            with self.assertRaises(FileExistsError):
                with publication_lock_local(root, "development", "different-dataset-run"):
                    self.fail("Conflicting publisher acquired table lock")
            with publication_lock_local(root, "production", "other-destination"):
                self.assertTrue((root / "production.lock").exists())
        self.assertFalse((root / "development.lock").exists())
        with self.assertRaisesRegex(RuntimeError, "body failure"):
            with publication_lock_local(root, "development", "run"):
                raise RuntimeError("body failure")
        self.assertFalse((root / "development.lock").exists())

    def test_lock_does_not_steal_or_release_changed_owner(self):
        root = self.root / "locks"
        with self.assertRaisesRegex(ValueError, "ownership changed"):
            with publication_lock_local(root, "development", "run"):
                lock = root / "development.lock"
                lock.write_text(json.dumps(dict(run_id="someone-else", token="different")), encoding="utf-8")
        self.assertTrue((root / "development.lock").exists())
        with self.assertRaises(FileExistsError):
            with publication_lock_local(root, "development", "retry"):
                self.fail("Stale lock was stolen")


if __name__ == "__main__":
    unittest.main()
