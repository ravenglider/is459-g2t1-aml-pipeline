"""Fake S3 protocol tests and separately labelled REAL local Spark/Parquet tests.

No SDK clients/network/emulator/credentials. Local transport maps S3 URIs to a
temporary directory: it does not test Glue's S3 filesystem or commit protocol.
"""

from copy import deepcopy
from datetime import datetime, timezone, timedelta
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import sequence_s3 as s3
from sequence_manifest import create_run_manifest, transition_manifest


class FakeError(RuntimeError):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    """Deliberately small protocol fake with pagination, CAS and lost responses."""
    def __init__(self):
        self.objects = {}
        self.calls = []
        self.serial = 0
        self.lose_response = False
        self.fail_before = False

    def seed(self, key, raw=b"parquet-placeholder"):
        self.serial += 1
        etag = '"opaque-' + str(self.serial) + '"'
        self.objects[key] = dict(raw=raw, ETag=etag,
            LastModified=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=self.serial))
        return etag

    def put_object(self, **kwargs):
        self.calls.append(("put", deepcopy(kwargs)))
        if kwargs["Bucket"] != s3.BUCKET:
            raise AssertionError("wrong bucket")
        key = kwargs["Key"]
        old = self.objects.get(key)
        if kwargs.get("IfNoneMatch") == "*" and old is not None:
            raise FakeError("PreconditionFailed")
        if "IfMatch" in kwargs and (old is None or old["ETag"] != kwargs["IfMatch"]):
            raise FakeError("PreconditionFailed")
        if "IfNoneMatch" not in kwargs and "IfMatch" not in kwargs:
            raise AssertionError("unconditional metadata write")
        if self.fail_before:
            self.fail_before = False
            raise TimeoutError("not committed")
        etag = self.seed(key, kwargs["Body"])
        if self.lose_response:
            self.lose_response = False
            raise TimeoutError("committed, response lost")
        return dict(ETag=etag)

    def get_object(self, **kwargs):
        self.calls.append(("get", kwargs.copy()))
        if kwargs["Key"] not in self.objects:
            raise FakeError("NoSuchKey")
        obj = self.objects[kwargs["Key"]]
        return dict(Body=io.BytesIO(obj["raw"]), ETag=obj["ETag"])

    def list_objects_v2(self, **kwargs):
        self.calls.append(("list", kwargs.copy()))
        keys = sorted(key for key in self.objects if key.startswith(kwargs["Prefix"]))
        start = int(kwargs.get("ContinuationToken", 0))
        page = keys[start:start + 2]
        contents = [dict(Key=key, Size=len(self.objects[key]["raw"]),
                         LastModified=self.objects[key]["LastModified"], ETag=self.objects[key]["ETag"]) for key in page]
        response = dict(Contents=contents, IsTruncated=start + 2 < len(keys))
        if response["IsTruncated"]:
            response["NextContinuationToken"] = str(start + 2)
        return response


def settings():
    return dict(dataset="sample", destination="development", start_date="2022-09-01", timezone="UTC",
                timezone_verified=True, timezone_evidence="SYNTHETIC evidence only", expected_counts={2: 1, 3: 0},
                source_fingerprint="a" * 64)


def manifest():
    return create_run_manifest("run", "development", "sample", "full", ["2022-09-01"],
        builder=dict(version="synthetic", dirty=False, code_fingerprint="c" * 64),
        timezone="UTC", timezone_evidence="synthetic independent upstream verification",
        source_coverage=dict(start_date="2022-09-01", end_date="2022-09-01", verified=True, evidence="synthetic coverage"),
        source_inventory=dict(fingerprint="a" * 64, evidence="synthetic inventory"),
        identity_check=dict(dataset="sample", row_count=2, identity_check_passed=True,
                            verification_scope="provided_dataframe_only", dataset_wide_verified=False),
        coverage_evidence=dict(verified=True, dataset="sample", checked_dates=["2022-09-01"],
                               source_inventory_fingerprint="a" * 64, evidence="synthetic whole sample scan"),
        previous_publication=None, expected_current_publication_fingerprint="b" * 64)


class FakeProtocolTests(unittest.TestCase):
    """No Spark JVM: protocol tests mock analytical validation explicitly."""
    def setUp(self):
        self.client = FakeS3()

    def reserve(self, attempt="attempt", **changes):
        return s3.reserve_staging_attempt_s3(self.client, "run", attempt, **dict(settings(), **changes))

    def stage(self, attempt="attempt", empty=False, write_error=None, validate_error=None):
        cfg = settings()
        if empty:
            cfg["expected_counts"] = {2: 0, 3: 0}
        report = dict(counts=cfg["expected_counts"], row_count=sum(cfg["expected_counts"].values()),
                      schema_valid=True, values_valid=True)
        transport, output = MagicMock(), MagicMock()
        def write(frame, uri, maximum):
            key = s3._key(uri) + "dataset=sample/start_date=2022-09-01/part.snappy.parquet"
            self.client.seed(key)
            if write_error:
                raise write_error
        transport.write.side_effect = write
        with patch("sequence_staging.validate_sequence_output", side_effect=[report, validate_error or report]):
            uri = s3.stage_sequence_batch_s3(self.client, output, "run", attempt,
                **cfg, max_records_per_file=2, transport=transport)
        return uri, transport

    def test_reservation_conflict_prefix_safety_and_no_takeover(self):
        self.reserve()
        with self.assertRaises(ValueError):
            self.reserve()
        for changes in (dict(dataset="HI-Medium"), dict(dataset="LI-Medium"), dict(destination="production"),
                        dict(timezone_verified=False), dict(start_date="20220901")):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.reserve("fresh", **changes)
        for attempt in ("../x", "a/b", "", "a%2Fb"):
            with self.assertRaises(ValueError):
                self.reserve(attempt)
        for uri in ("s3://other/bq2_sequences_staging/", s3.ROOT + "bq2_sequences/",
                    s3.ROOT + "bq2_sequences_dev/", s3.STAGING + "../x/", s3.SOURCE + "%2F/",
                    s3.ROOT + "transactions_clean/dataset=HI-Medium/"):
            with self.assertRaises(ValueError):
                s3.inventory_s3_objects(self.client, uri)

    def test_conditional_race_owner_revision_and_stale_etag(self):
        reservation = self.reserve()
        old = deepcopy(reservation)
        s3._advance(self.client, reservation, "VALIDATING")
        with self.assertRaises(ValueError):
            s3._advance(self.client, old, "FAILED")
        forged = deepcopy(reservation)
        forged["status"]["owner_token"] = "other"
        with self.assertRaises(ValueError):
            s3._advance(self.client, forged, "FAILED")
        with self.assertRaisesRegex(ValueError, "conflict"):
            s3._put(self.client, reservation["uri"] + "metadata/status.json", old["status"], old["etag"])
        with self.assertRaises(ValueError):
            s3._advance(self.client, reservation, "WRITING")
        # Two callers both saw empty; conditional creation still chooses one.
        with patch.object(self.client, "list_objects_v2", return_value={"IsTruncated": False}):
            with self.assertRaisesRegex(ValueError, "conflict"):
                self.reserve()

    def test_lost_response_recovery_requires_exact_intended_bytes(self):
        self.client.lose_response = True
        reservation = self.reserve()
        self.assertEqual(reservation["status"]["state"], "WRITING")
        self.client.lose_response = True
        s3._advance(self.client, reservation, "VALIDATING")
        self.assertEqual(reservation["status"]["revision"], 1)
        self.client.fail_before = True
        with self.assertRaises(TimeoutError):
            self.reserve("not_committed")
        with patch.object(self.client, "put_object", side_effect=TimeoutError("unknown outcome")):
            with self.assertRaises(TimeoutError):
                s3._put(self.client, reservation["uri"] + "metadata/status.json", dict(reservation["status"], revision=20), reservation["etag"])

    def test_inventory_pagination_sorting_and_added_removed_changed_files(self):
        prefix = s3.STAGING + "run/attempt/data/"
        for name in ("c", "a", "b", "d", "e"):
            self.client.seed(s3._key(prefix) + name)
        records = s3.inventory_s3_objects(self.client, prefix)
        self.assertEqual(len(records), 5)
        self.assertEqual(records, sorted(records, key=lambda r: r["path"]))
        self.assertGreaterEqual(sum(kind == "list" for kind, _ in self.client.calls), 3)
        fp = s3._fingerprint(records)
        self.assertEqual(fp, s3._fingerprint(list(reversed(records))))
        self.client.seed(s3._key(prefix) + "f")
        self.assertNotEqual(fp, s3._fingerprint(s3.inventory_s3_objects(self.client, prefix)))
        del self.client.objects[s3._key(prefix) + "f"]
        self.client.seed(s3._key(prefix) + "a", b"changed")
        self.assertNotEqual(fp, s3._fingerprint(s3.inventory_s3_objects(self.client, prefix)))
        del self.client.objects[s3._key(prefix) + "b"]
        self.assertEqual(len(s3.inventory_s3_objects(self.client, prefix)), 4)

    def test_duplicate_bad_metadata_and_broken_pagination_rejected(self):
        prefix = s3.STAGING + "run/attempt/data/"
        item = dict(Key=s3._key(prefix) + "x", Size=1, LastModified=datetime.now(timezone.utc), ETag='"x"')
        for response in (dict(Contents=[item, item]), dict(Contents=[dict(item, Size=-1)]),
                         dict(Contents=[dict(item, LastModified=datetime.now())]),
                         dict(IsTruncated=True), dict(Contents=[dict(item, Key="outside")])):
            with patch.object(self.client, "list_objects_v2", return_value=response), self.assertRaises(ValueError):
                s3.inventory_s3_objects(self.client, prefix)

    def test_success_receipts_empty_results_and_fresh_attempt_retries(self):
        uri, transport = self.stage()
        evidence = s3._load_validated(self.client, uri)
        self.assertEqual(evidence["counts"], {"2": 1, "3": 0})
        self.assertEqual(evidence["status"]["state"], "VALIDATED")
        self.assertEqual(transport.write.call_count, 1)
        empty, empty_transport = self.stage("empty", empty=True)
        empty_transport.write.assert_not_called()
        self.assertEqual(s3._load_validated(self.client, empty)["counts"], {"2": 0, "3": 0})
        self.assertTrue(s3.inventory_s3_objects(self.client, uri + "data/"))
        self.assertEqual(s3.inventory_s3_objects(self.client, empty + "data/"), [])
        with self.assertRaises(ValueError):
            self.stage()

    def test_failed_writes_validation_and_interrupted_attempts_preserved(self):
        with self.assertRaisesRegex(RuntimeError, "partial"):
            self.stage(write_error=RuntimeError("partial write"))
        uri = s3.STAGING + "run/attempt/"
        status, _, _ = s3._get(self.client, uri + "metadata/status.json")
        self.assertEqual(status["state"], "FAILED")
        self.assertTrue(s3.inventory_s3_objects(self.client, uri + "data/"))
        with self.assertRaises(ValueError):
            s3._load_validated(self.client, uri)
        with self.assertRaisesRegex(ValueError, "invalid rows"):
            self.stage("bad_values", validate_error=ValueError("invalid rows"))
        self.stage("retry")
        reservation = self.reserve("interrupted")
        with self.assertRaisesRegex(ValueError, "not VALIDATED"):
            s3._load_validated(self.client, reservation["uri"])

    def test_success_marker_alone_missing_malformed_and_changed_receipts(self):
        reservation = self.reserve()
        self.client.seed(s3._key(reservation["uri"] + "data/_SUCCESS"), b"")
        with self.assertRaises(ValueError):
            s3._load_validated(self.client, reservation["uri"])
        uri, _ = self.stage("good")
        key = s3._key(uri + "metadata/validation.json")
        saved = deepcopy(self.client.objects[key])
        self.client.seed(key, b"malformed")
        with self.assertRaises(ValueError):
            s3._load_validated(self.client, uri)
        self.client.objects[key] = saved
        self.client.seed(s3._key(uri + "data/dataset=sample/start_date=2022-09-01/extra.parquet"))
        with self.assertRaises(ValueError):
            s3._load_validated(self.client, uri)
        del self.client.objects[s3._key(uri + "data/dataset=sample/start_date=2022-09-01/extra.parquet")]
        self.client.seed(s3._key(uri + "metadata/leftover.tmp"))
        with self.assertRaises(ValueError):
            s3._load_validated(self.client, uri)
        del self.client.objects[key]
        with self.assertRaises(FakeError):
            s3._load_validated(self.client, uri)

    def test_manifest_attachment_readiness_conditional_updates_and_idempotence(self):
        uri, _ = self.stage()
        original = manifest()
        owner = "owner"
        etag = s3.write_manifest_s3(self.client, original, owner_token=owner)
        staged = s3.attach_validated_attempt_s3(self.client, original, uri)
        next_etag = s3.write_manifest_s3(self.client, staged, owner_token=owner,
                                       expected_revision=0, expected_etag=etag)
        self.assertEqual(next_etag, s3.write_manifest_s3(self.client, staged, owner_token=owner,
                                                       expected_revision=0, expected_etag=etag))
        with self.assertRaises(ValueError):
            s3.attach_validated_attempt_s3(self.client, staged, uri)
        loader = lambda path: s3._load_validated(self.client, path)
        ready = transition_manifest(staged, "READY_TO_PLAN", current_source_fingerprint="a" * 64, receipt_loader=loader)
        self.client.lose_response = True
        s3.write_manifest_s3(self.client, ready, owner_token=owner, expected_revision=staged["revision"],
                             expected_etag=next_etag, current_source_fingerprint="a" * 64)
        self.assertEqual(ready["totals"]["row_count"], 1)
        self.assertFalse(ready["identity_check"]["dataset_wide_verified"])
        for changes in (dict(owner_token="other"), dict(expected_etag='"stale"'), dict(expected_revision=99)):
            options = dict(owner_token=owner, expected_revision=ready["revision"],
                           expected_etag=s3._get(self.client, s3.METADATA + "run/manifest.json")[1])
            options.update(changes)
            failed = transition_manifest(ready, "FAILED", error="synthetic failure")
            with self.assertRaises(ValueError):
                s3.write_manifest_s3(self.client, failed, **options)

    def test_manifest_refuses_publication_wrong_population_source_and_fake_totals(self):
        uri, _ = self.stage()
        original = manifest()
        for changes in (dict(state="PLANNED"), dict(state="PUBLISHED"), dict(dataset="HI-Medium"),
                        dict(destination="production")):
            with self.assertRaises(ValueError):
                s3.write_manifest_s3(self.client, dict(original, **changes), owner_token="owner")
        changed = deepcopy(original)
        changed["source_inventory"]["fingerprint"] = "f" * 64
        with self.assertRaises(ValueError):
            s3.attach_validated_attempt_s3(self.client, changed, uri)
        with self.assertRaises(ValueError):
            s3.write_manifest_s3(self.client, dict(original, state="READY_TO_PLAN"), owner_token="owner")

    def test_receipt_failure_never_marks_validated_and_preserves_failed_attempt(self):
        actual_put = self.client.put_object
        def fail_validation(**kwargs):
            if kwargs["Key"].endswith("metadata/validation.json"):
                raise RuntimeError("receipt storage unavailable")
            return actual_put(**kwargs)
        with patch.object(self.client, "put_object", side_effect=fail_validation):
            with self.assertRaisesRegex(RuntimeError, "receipt storage"):
                self.stage()
        uri = s3.STAGING + "run/attempt/"
        status, _, _ = s3._get(self.client, uri + "metadata/status.json")
        self.assertEqual(status["state"], "FAILED")
        self.assertTrue(s3.inventory_s3_objects(self.client, uri + "data/"))
        self.assertNotIn(s3._key(uri + "metadata/validation.json"), self.client.objects)
        self.stage("new_attempt")

    def test_conflicting_final_status_is_not_overwritten_by_failure_reporting(self):
        actual_put = self.client.put_object
        def steal_status(**kwargs):
            value = json.loads(kwargs["Body"])
            if kwargs["Key"].endswith("metadata/status.json") and value["state"] == "VALIDATED":
                changed = dict(value, owner_token="different_owner", state="VALIDATING")
                self.client.seed(kwargs["Key"], s3._bytes(changed))
            return actual_put(**kwargs)
        with patch.object(self.client, "put_object", side_effect=steal_status):
            with self.assertRaisesRegex(ValueError, "conflict") as caught:
                self.stage()
        self.assertTrue(any("Could not record FAILED" in note for note in caught.exception.__notes__))
        status, _, _ = s3._get(self.client, s3.STAGING + "run/attempt/metadata/status.json")
        self.assertEqual(status["owner_token"], "different_owner")

    def test_inconsistent_count_receipt_and_unknown_artifacts_rejected(self):
        uri, _ = self.stage()
        status_uri, report_uri = uri + "metadata/status.json", uri + "metadata/validation.json"
        report, _, _ = s3._get(self.client, report_uri)
        report["row_count"] = 99
        raw = s3._bytes(report)
        self.client.seed(s3._key(report_uri), raw)
        status, _, _ = s3._get(self.client, status_uri)
        status["receipt_fingerprints"]["validation"] = s3._sha(raw)
        self.client.seed(s3._key(status_uri), s3._bytes(status))
        with self.assertRaisesRegex(ValueError, "Inconsistent"):
            s3._load_validated(self.client, uri)

    def test_wrong_partition_and_temporary_artifacts_fail_before_acceptance(self):
        for path in ("data/_temporary/part.parquet", "data/dataset=LI-Medium/start_date=2022-09-01/p.parquet",
                     "data/dataset=sample/start_date=2022-09-02/p.parquet"):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "Unexpected"):
                s3._data_files(s3.STAGING + "run/attempt/", [dict(path=s3.STAGING + "run/attempt/" + path)],
                               dict(start_date="2022-09-01", expected_counts={2: 1, 3: 0}))

    def test_data_change_during_validation_blocks_final_success(self):
        transport, output = MagicMock(), MagicMock()
        uri = s3.STAGING + "run/changing/"
        def write(*args):
            self.client.seed(s3._key(uri + "data/dataset=sample/start_date=2022-09-01/a.parquet"))
        def read(*args):
            self.client.seed(s3._key(uri + "data/dataset=sample/start_date=2022-09-01/b.parquet"))
            return output
        transport.write.side_effect = write
        transport.read.side_effect = read
        report = dict(counts={2: 1, 3: 0}, row_count=1, schema_valid=True, values_valid=True)
        with patch("sequence_staging.validate_sequence_output", return_value=report):
            with self.assertRaisesRegex(ValueError, "changed during validation"):
                s3.stage_sequence_batch_s3(self.client, output, "run", "changing", **settings(),
                                           max_records_per_file=1, transport=transport)
        status, _, _ = s3._get(self.client, uri + "metadata/status.json")
        self.assertEqual(status["state"], "FAILED")


class LocalTransport(s3._SparkParquetTransport):
    """REAL Parquet files, FAKE S3 inventory. Not an AWS filesystem simulation."""
    def __init__(self, root, client):
        self.root, self.client = root, client

    def path(self, uri):
        return (self.root / s3._key(uri)).as_posix()

    def sync(self, uri):
        base = Path(self.path(uri))
        for file in base.rglob("*"):
            if file.is_file() and not file.name.endswith(".crc"):
                self.client.seed(file.relative_to(self.root).as_posix(), file.read_bytes())

    def write(self, output, uri, maximum):
        super().write(output, uri, maximum)
        self.sync(uri)


class RealLocalParquetTests(unittest.TestCase):
    """Requires local Java/Spark filesystem support; run unchanged under WSL2."""
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        from fixtures import BRANCHING_TRIPLE_EXPECTED, OUTPUT_SCHEMA, BIGINT_IDS
        previous = os.environ.get("PYSPARK_PYTHON")
        os.environ["PYSPARK_PYTHON"] = sys.executable
        def restore():
            if previous is None:
                os.environ.pop("PYSPARK_PYTHON", None)
            else:
                os.environ["PYSPARK_PYTHON"] = previous
        cls.addClassCleanup(restore)
        cls.spark = (SparkSession.builder.master("local[2]").appName("role5-fake-s3-real-parquet")
            .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false").getOrCreate())
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        row = deepcopy(BRANCHING_TRIPLE_EXPECTED)
        row["txn_timestamps"] = [ts.replace(tzinfo=timezone.utc) for ts in row["txn_timestamps"]]
        row["txn_ids"] = list(BIGINT_IDS)
        row["final_txn_id"] = BIGINT_IDS[-1]
        row["sequence_id"] = hashlib.sha256(("v1\nsample\n" + ",".join(map(str, BIGINT_IDS))).encode()).hexdigest()
        cls.output = cls.spark.createDataFrame([row], OUTPUT_SCHEMA)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="role5-fake-s3-")
        self.addCleanup(self.temp.cleanup)
        self.client = FakeS3()
        self.transport = LocalTransport(Path(self.temp.name), self.client)

    def stage(self, attempt="real", output=None, **changes):
        cfg = dict(settings(), expected_counts={2: 0, 3: 1})
        cfg.update(changes)
        return s3.stage_sequence_batch_s3(self.client, self.output if output is None else output,
            "run", attempt, **cfg, max_records_per_file=1, transport=self.transport)

    def read(self, uri):
        return s3.read_staged_batch_s3(self.client, self.spark, uri,
            timezone_verified=True, timezone_evidence="synthetic only", transport=self.transport)

    def test_real_snappy_roundtrip_all_21_columns_bigints_and_partition_strings(self):
        uri = self.stage()
        actual = self.read(uri)
        self.assertEqual(actual.schema.simpleString(), self.output.schema.simpleString())
        self.assertEqual(actual.collect(), self.output.collect())
        records = s3.inventory_s3_objects(self.client, uri + "data/")
        self.assertTrue(any("dataset=sample/start_date=2022-09-01/" in r["path"] for r in records))

    def test_real_empty_attempt_is_explicit_and_cannot_retain_previous_data(self):
        nonempty = self.stage()
        empty = self.stage("empty", self.output.limit(0), expected_counts={2: 0, 3: 0})
        self.assertEqual(self.read(empty).count(), 0)
        self.assertEqual(self.read(nonempty).count(), 1)
        self.assertEqual(s3.inventory_s3_objects(self.client, empty + "data/"), [])

    def test_real_incompatible_physical_schema_is_rejected_without_casting(self):
        from pyspark.sql import functions as F
        uri = s3.STAGING + "run/wrong/data/"
        self.transport.write(self.output.withColumn("final_txn_id", F.col("final_txn_id").cast("double")), uri, 1)
        records = s3.inventory_s3_objects(self.client, uri)
        files = [r["path"] for r in records if r["path"].endswith(".parquet")]
        with self.assertRaisesRegex(ValueError, "physical"):
            self.transport.read(self.spark, uri, files, dict(timezone="UTC"))


if __name__ == "__main__":
    unittest.main()
