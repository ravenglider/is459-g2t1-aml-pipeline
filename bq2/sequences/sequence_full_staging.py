"""Isolated HI/LI immutable staging; no production writes or Catalog mutation.

Reuses tested conditional JSON primitives and Spark Parquet validation transport.
Every run/attempt owns its prefix. Partial/failed attempts are never reused.
"""
from copy import deepcopy

from sequence_manifest import _token, _now, _digest
from sequence_s3 import _put, _get, _SparkParquetTransport
from sequence_full_source import BUCKET, dataset_spec, inventory_objects

STAGING_ROOT = f"s3://{BUCKET}/bq2_sequences_staging/full/"


class FullS3Stager:
    """Injected storage interface for generation; receipt creation is conditional.

    Successful staging does not imply publication. Neither final bq2_sequences/
    nor the completed sample development locations can be written here.
    """
    def __init__(self, client, config, *, transport=None):
        dataset_spec(config["dataset"])
        self.client = client
        self.config = deepcopy(config)
        self.root = (STAGING_ROOT + "dataset=" + config["dataset"] + "/"
                     + _token(config["run_id"]) + "/" + _token(config["attempt_id"]) + "/")
        self.transport = transport or _SparkParquetTransport()
        self.manifest = None
        self.etag = None

    def _write_run(self, manifest):
        uri = self.root + "metadata/run.json"
        etag = _put(self.client, uri, manifest, self.etag)
        observed, observed_etag, _ = _get(self.client, uri)
        if observed != manifest or observed_etag != etag:
            raise ValueError("Unconfirmed full-run manifest write")
        self.manifest, self.etag = deepcopy(manifest), etag

    def reserve(self, manifest):
        if self.manifest is not None or inventory_objects(self.client, self.root):
            raise ValueError("Full run/attempt already exists; choose a fresh attempt")
        self._write_run(manifest)

    def _assert_owner(self):
        if self.manifest is None:
            raise ValueError("Run must be durably reserved before staging")
        observed, etag, _ = _get(self.client, self.root + "metadata/run.json")
        if observed != self.manifest or etag != self.etag:
            raise ValueError("Full run ownership/revision conflict")

    def stage(self, output, *, start_date, expected_counts):
        from sequence_source import fingerprint_source_inventory
        from sequence_processing import _iso_date
        from sequence_staging import validate_sequence_output
        self._assert_owner()
        day = _iso_date(start_date, "start_date").isoformat()
        if day not in self.manifest["owner_dates"]:
            raise ValueError("Batch not owned by this run")
        root = self.root + "batches/" + day + "/"
        if inventory_objects(self.client, root):
            raise ValueError("Batch already exists; no overwrite/resume")
        dataset = self.config["dataset"]
        report = validate_sequence_output(output, dataset, day, "UTC", expected_counts)
        status = dict(format_version=1, state="WRITING", revision=0, dataset=dataset, start_date=day,
            timezone="UTC", owner_token=self.config["owner_token"], run_id=self.config["run_id"],
            attempt_id=self.config["attempt_id"], expected_counts={str(k):v for k,v in expected_counts.items()})
        status_uri = root + "metadata/status.json"
        etag = _put(self.client, status_uri, status)

        def advance(state):
            nonlocal status, etag
            actual, actual_etag, _ = _get(self.client, status_uri)
            if actual != status or actual_etag != etag:
                raise ValueError("Batch ownership changed")
            updated = dict(status, state=state, revision=status["revision"]+1)
            etag = _put(self.client, status_uri, updated, etag)
            actual, actual_etag, _ = _get(self.client, status_uri)
            if actual != updated or actual_etag != etag:
                raise ValueError("Unconfirmed batch receipt")
            status = updated

        try:
            if report["row_count"]:
                self.transport.write(output, root+"data/", self.config["max_records_per_file"])
            advance("VALIDATING")
            records = inventory_objects(self.client, root+"data/")
            files = []
            prefix = root + "data/dataset=" + dataset + "/start_date=" + day + "/"
            for item in records:
                path = item["path"]
                if path == root+"data/_SUCCESS":
                    continue
                if (not path.startswith(prefix) or "/" in path[len(prefix):]
                        or not path.endswith(".parquet") or item["size"] <= 0):
                    raise ValueError("Unexpected full batch data artifact")
                files.append(path)
            if bool(files) != bool(report["row_count"]):
                raise ValueError("Explicit empty/nonempty batch disagrees with files")
            back = self.transport.read(output.sparkSession, root+"data/", files, status)
            validation = validate_sequence_output(back, dataset, day, "UTC", expected_counts)
            if inventory_objects(self.client, root+"data/") != records:
                raise ValueError("Staged inventory changed during read-back")
            allowed = {status_uri} | {r["path"] for r in records}
            if any(r["path"] not in allowed for r in inventory_objects(self.client, root)):
                raise ValueError("Unexpected temporary batch artifact")
            receipt = dict(dataset=dataset, start_date=day, run_id=self.config["run_id"],
                attempt_id=self.config["attempt_id"], staging_location=root+"data/", files=records,
                inventory_fingerprint=fingerprint_source_inventory(records),
                parquet_files=len(files), size_bytes=sum(r["size"] for r in records),
                **validation, validation_status="VALIDATED", source=self.manifest["source"],
                schema=self.manifest["schema"], rule_version="v1", builder=self.manifest["builder"])
            receipt["counts"] = {str(k):v for k,v in validation["counts"].items()}
            _put(self.client, root+"metadata/validation.json", receipt)
            advance("VALIDATED")
            loaded, _, raw = _get(self.client, root+"metadata/validation.json")
            if loaded != receipt:
                raise ValueError("Validation receipt changed")
            self._assert_owner()
            return dict(receipt, receipt_uri=root+"metadata/validation.json", receipt_fingerprint=_digest(loaded))
        except Exception as original:
            try:
                if status["state"] != "VALIDATED":
                    advance("FAILED")
            except Exception as recording:
                original.add_note("Could not mark batch FAILED: "+type(recording).__name__)
            raise

    def finish(self, manifest):
        self._assert_owner()
        if manifest["state"] != "STAGED_VALIDATED" or manifest.get("published") is not False:
            raise ValueError("Only staging completion is supported")
        for batch in manifest["batches"]:
            receipt, _, _ = _get(self.client, batch["receipt_uri"])
            if (_digest(receipt) != batch["receipt_fingerprint"]
                    or inventory_objects(self.client, batch["staging_location"]) != batch["files"]):
                raise ValueError("Previously validated batch evidence changed")
            status, _, _ = _get(self.client, batch["receipt_uri"].replace("validation.json", "status.json"))
            if status.get("state") != "VALIDATED" or status.get("owner_token") != self.config["owner_token"]:
                raise ValueError("Batch status/ownership no longer valid")
        self._write_run(manifest)
        return dict(manifest=deepcopy(manifest), manifest_uri=self.root+"metadata/run.json", etag=self.etag)

    def fail(self, error):
        # Never take over another owner's manifest or rewrite terminal success.
        self._assert_owner()
        if self.manifest["state"] != "CREATED":
            raise ValueError("Terminal full-run manifest cannot be rewritten")
        failed = dict(self.manifest, state="FAILED", error_type=type(error).__name__,
                      updated_at=_now(), published=False)
        self._write_run(failed)
