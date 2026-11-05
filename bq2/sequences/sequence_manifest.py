"""Local run evidence and replacement PLANS only; standard library, no Spark/AWS.

No partition files are copied/deleted and no catalog is touched. Receipts are
evidence, not independent Parquet revalidation. Before eventual publication,
revalidate staged data, source consistency and current-publication identity.
Previous locations are NOT backups: retention/restore must be designed first.
The local lock coordinates cooperative local writers, not AWS or hostile races.
"""

from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from uuid import uuid4


DESTINATIONS = {
    "development": ("is459_g2t1_aml.bq2_sequences_dev", "s3://is459-g2t1-aml-processed-bucket/bq2_sequences_dev/"),
    "production": ("is459_g2t1_aml.bq2_sequences", "s3://is459-g2t1-aml-processed-bucket/bq2_sequences/"),
}
DATASETS = ("sample", "HI-Medium", "LI-Medium")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()


def _token(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value):
        raise ValueError("Unsafe run/attempt identifier")
    return value


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("Expected a lowercase SHA-256 fingerprint")
    return value


def _day(value):
    try:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value:
            raise ValueError()
        return parsed
    except (TypeError, ValueError) as exc:
        raise ValueError("Expected canonical YYYY-MM-DD date") from exc


def _dates(values):
    if isinstance(values, (str, bytes)):
        raise ValueError("Expected a collection of dates")
    result = [_day(v).isoformat() for v in values]
    if len(result) != len(set(result)):
        raise ValueError("Duplicate dates are not allowed")
    return sorted(result)


def _range(first, last):
    a, b = _day(first), _day(last)
    if a > b:
        raise ValueError("Reversed source coverage")
    return [(a + timedelta(days=i)).isoformat() for i in range((b - a).days + 1)]


def _counts(values):
    if not isinstance(values, dict) or set(values) not in ({2, 3}, {"2", "3"}):
        raise ValueError("Counts must explicitly include lengths 2 and 3")
    result = {str(k): v for k, v in values.items()}
    if any(type(v) is not int or v < 0 for v in result.values()):
        raise ValueError("Counts must be nonnegative integers")
    return result


def _local(value):
    if not isinstance(value, (str, os.PathLike)):
        raise ValueError("Expected an absolute local metadata path")
    path = Path(value).expanduser()
    if "://" in str(value) or str(value).startswith(("//", "\\\\")) or not path.is_absolute() or ".." in path.parts:
        raise ValueError("Expected an absolute local metadata path without '..'")
    for part in (path, *path.parents):
        try:
            attributes = getattr(part.lstat(), "st_file_attributes", 0)
        except FileNotFoundError:
            attributes = 0
        if part.is_symlink() or attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            raise ValueError("Symlinks/reparse points are not allowed")
    return path.resolve()


def _evidence(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} requires an independent evidence reference")


def _previous(value):
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {"run_id", "manifest_fingerprint"}:
        raise ValueError("Previous publication requires run_id and manifest_fingerprint")
    _token(value["run_id"])
    _hash(value["manifest_fingerprint"])
    return deepcopy(value)


def _check_manifest(manifest):
    destination, dataset = manifest.get("destination"), manifest.get("dataset")
    if destination not in DESTINATIONS or dataset not in DATASETS or (destination == "production" and dataset == "sample"):
        raise ValueError("Manifest destination/dataset isolation violated")
    table, root = DESTINATIONS[destination]
    if (manifest.get("table"), manifest.get("output_root"), manifest.get("rule_version")) != (table, root, "v1"):
        raise ValueError("Manifest destination mapping or rule version changed")
    _token(manifest["run_id"])
    if type(manifest.get("revision")) is not int or manifest["revision"] < 0:
        raise ValueError("Invalid manifest revision")
    coverage = manifest["source_coverage"]
    if coverage.get("verified") is not True:
        raise ValueError("Source boundaries must remain verified")
    complete = _range(coverage["start_date"], coverage["end_date"])
    requested = _dates(manifest["start_dates"])
    if not requested or not set(requested) <= set(complete):
        raise ValueError("Invalid requested coverage")
    if manifest["refresh_scope"] not in ("full", "partial") or (manifest["refresh_scope"] == "full" and requested != complete):
        raise ValueError("Full versus partial coverage mismatch")


def create_run_manifest(run_id, destination, dataset, refresh_scope, start_dates, *,
                        builder, timezone, timezone_evidence, source_coverage,
                        source_inventory, identity_check, coverage_evidence,
                        previous_publication, expected_current_publication_fingerprint,
                        upstream_changed_dates=(), repair_claim="none"):
    """Create one run; evidence is caller-supplied, never inferred from a slice.

    builder requires version, dirty (bool), and actual code_fingerprint, including
    dirty content. No Git commands are run. coverage_evidence must link the full
    dataset identity scan to this inventory; the original check stays unmodified.
    """
    _token(run_id)
    if destination not in DESTINATIONS or dataset not in DATASETS or (destination == "production" and dataset == "sample"):
        raise ValueError("Invalid destination/dataset combination")
    if refresh_scope not in ("full", "partial"):
        raise ValueError("refresh_scope must be full or partial")
    if not isinstance(builder, dict) or type(builder.get("dirty")) is not bool:
        raise ValueError("Builder must record version, dirty flag and actual code fingerprint")
    _evidence(builder.get("version"), "builder version")
    _hash(builder.get("code_fingerprint"))
    _evidence(timezone, "timezone")
    _evidence(timezone_evidence, "timezone")
    if source_coverage.get("verified") is not True:
        raise ValueError("Source boundaries must be independently verified")
    _evidence(source_coverage.get("evidence"), "source coverage")
    complete = _range(source_coverage["start_date"], source_coverage["end_date"])
    requested = _dates(start_dates)
    if not requested or not set(requested) <= set(complete):
        raise ValueError("Requested dates must be nonempty and within source coverage")
    if refresh_scope == "full" and requested != complete:
        raise ValueError("Full refresh must account for every covered start date")
    _hash(source_inventory.get("fingerprint"))
    _evidence(source_inventory.get("evidence"), "source inventory")
    _hash(expected_current_publication_fingerprint)
    changes = _dates(upstream_changed_dates)
    if not set(changes) <= set(complete):
        raise ValueError("Changed source dates must lie within confirmed coverage")
    if repair_claim not in ("none", "bounded", "exhaustive"):
        raise ValueError("Invalid repair claim")
    affected = sorted({date.fromordinal(max(_day(source_coverage["start_date"]).toordinal(), _day(d).toordinal() - i)).isoformat()
                       for d in changes for i in range(3)})
    missing = sorted(set(affected) - set(requested))
    if changes and refresh_scope == "partial" and (repair_claim == "none" or (repair_claim == "exhaustive" and missing)):
        raise ValueError("Partial source repair must disclose bounded scope or cover all affected earlier dates")
    table, root = DESTINATIONS[destination]
    return dict(format_version=1, run_id=run_id, destination=destination, table=table, output_root=root,
                dataset=dataset, refresh_scope=refresh_scope, start_dates=requested, rule_version="v1",
                builder=deepcopy(builder), timezone=timezone, timezone_evidence=timezone_evidence,
                source_coverage=deepcopy(source_coverage), source_inventory=deepcopy(source_inventory),
                identity_check=deepcopy(identity_check), coverage_evidence=deepcopy(coverage_evidence),
                repair=dict(changed_dates=changes, required_start_dates=affected, missing_start_dates=missing,
                            claim=repair_claim, exhaustive=not missing),
                mixed_source_versions_possible=refresh_scope == "partial", batches={}, totals=None,
                previous_publication=_previous(previous_publication),
                expected_current_publication_fingerprint=expected_current_publication_fingerprint,
                requirements=dict(reader_coordination="Wait for Athena/Role 6 queries and pause readers before destructive publication",
                                  writer_lock_scope=table,
                                  recovery="Previous locations are references, NOT backups; retention/restore must be designed first",
                                  source_snapshot="Inventory equality is not a transactional snapshot or refresh lock"),
                state="CREATED", revision=0, created_at=_now(), updated_at=_now(), plan=None)


def _attempt_receipts(path):
    attempt = _local(path)
    _token(attempt.name)
    _token(attempt.parent.name)
    if not attempt.is_dir():
        raise ValueError("Staging attempt is missing")
    for entry in attempt.rglob("*"):
        _local(entry)
        if entry.name.startswith("_temporary") or entry.name.endswith(".tmp"):
            raise ValueError("Incomplete staging attempt")
    records, fingerprints = {}, {}
    for name in ("status", "validation"):
        try:
            raw = (attempt / "metadata" / f"{name}.json").read_bytes()
            records[name] = json.loads(raw)
        except (OSError, ValueError) as exc:
            raise ValueError("Missing or invalid staging receipts") from exc
        if not isinstance(records[name], dict):
            raise ValueError("Staging receipts must be JSON objects")
        fingerprints[name] = hashlib.sha256(raw).hexdigest()
    status, report = records["status"], records["validation"]
    if status.get("format_version") != 1 or status.get("state") != "VALIDATED":
        raise ValueError("Attempt must be VALIDATED")
    counts = _counts(status.get("expected_counts"))
    if (_counts(report.get("counts")) != counts or type(report.get("row_count")) is not int
            or report.get("row_count") != sum(counts.values())
            or report.get("schema_valid") is not True or report.get("values_valid") is not True):
        raise ValueError("Inconsistent staging validation counts/results")
    return attempt, status, counts, fingerprints


def _bump(manifest):
    manifest["revision"] += 1
    manifest["updated_at"] = _now()
    return manifest


def _receipt_evidence(path, receipt_loader=None):
    """Storage seam: injected readers return verified, normalized receipt evidence.

    The shared ownership/count rules below still apply. Local callers retain the
    original filesystem checks; importing never constructs a storage client.
    """
    if receipt_loader is not None:
        return receipt_loader(path)
    attempt, status, counts, fingerprints = _attempt_receipts(path)
    return dict(run_id=attempt.parent.name, attempt_path=attempt.as_posix(),
                status=status, counts=counts, fingerprints=fingerprints,
                status_receipt=(attempt / "metadata" / "status.json").as_posix(),
                validation_receipt=(attempt / "metadata" / "validation.json").as_posix())


def attach_validated_attempt(manifest, attempt_path, *, expected_receipt_fingerprints=None, receipt_loader=None):
    """Return a new manifest selecting one attempt/date; do not revalidate Parquet."""
    _check_manifest(manifest)
    if manifest["state"] not in ("CREATED", "STAGING"):
        raise ValueError("Attempts can only be attached during staging")
    loaded = _receipt_evidence(attempt_path, receipt_loader)
    receipt, counts, fingerprints = loaded["status"], loaded["counts"], loaded["fingerprints"]
    day = receipt.get("start_date")
    if (loaded["run_id"] != manifest["run_id"] or day not in manifest["start_dates"]
            or receipt.get("dataset") != manifest["dataset"] or receipt.get("timezone") != manifest["timezone"]):
        raise ValueError("Attempt run/date/dataset/timezone ownership mismatch")
    if day in manifest["batches"]:
        raise ValueError("Duplicate selected staging attempt/date")
    if expected_receipt_fingerprints is not None and expected_receipt_fingerprints != fingerprints:
        raise ValueError("Staging receipt fingerprint mismatch")
    result = deepcopy(manifest)
    result["batches"][day] = dict(attempt_path=loaded["attempt_path"], start_date=day, counts=counts,
        row_count=sum(counts.values()), receipt_fingerprints=fingerprints,
        status_receipt=loaded["status_receipt"], validation_receipt=loaded["validation_receipt"])
    result["state"] = "STAGING"
    return _bump(result)


def validate_run_readiness(manifest, *, current_source_fingerprint, receipt_loader=None):
    """Require all dates and independent full-identity-coverage evidence.

    Evidence references are attestations supplied by the later coordinator. This
    module neither obtains inventories nor certifies those external attestations.
    """
    _check_manifest(manifest)
    if current_source_fingerprint != manifest["source_inventory"]["fingerprint"]:
        raise ValueError("Source inventory changed")
    report, coverage = manifest["identity_check"], manifest["coverage_evidence"]
    complete = _range(manifest["source_coverage"]["start_date"], manifest["source_coverage"]["end_date"])
    if (report.get("identity_check_passed") is not True or report.get("dataset") != manifest["dataset"]
            or coverage.get("verified") is not True or coverage.get("dataset") != manifest["dataset"]
            or coverage.get("source_inventory_fingerprint") != current_source_fingerprint
            or _dates(coverage.get("checked_dates", [])) != complete):
        raise ValueError("Separate dataset-wide identity coverage evidence is required")
    _evidence(coverage.get("evidence"), "identity scan coverage")
    if set(manifest["batches"]) != set(manifest["start_dates"]):
        raise ValueError("Missing selected staging dates, including explicit empty dates")
    totals = {"2": 0, "3": 0}
    for day, batch in manifest["batches"].items():
        loaded = _receipt_evidence(batch["attempt_path"], receipt_loader)
        receipt, counts, fingerprints = loaded["status"], loaded["counts"], loaded["fingerprints"]
        if (fingerprints != batch["receipt_fingerprints"] or counts != batch["counts"]
                or batch.get("start_date") != day or batch.get("row_count") != sum(counts.values())
                or batch.get("attempt_path") != loaded["attempt_path"]
                or batch.get("status_receipt") != loaded["status_receipt"]
                or batch.get("validation_receipt") != loaded["validation_receipt"]
                or receipt.get("dataset") != manifest["dataset"] or receipt.get("start_date") != day
                or receipt.get("timezone") != manifest["timezone"] or loaded["run_id"] != manifest["run_id"]):
            raise ValueError("Staging evidence changed or ownership mismatched")
        for length in totals:
            totals[length] += counts[length]
    return dict(counts=totals, row_count=sum(totals.values()), source_consistency_checked_at=_now(),
                source_fingerprint=current_source_fingerprint, receipts_verified=True,
                parquet_revalidation_required_before_publication=True)


def _check_complete_plan(manifest):
    plan = manifest.get("plan")
    if not isinstance(plan, dict) or not isinstance(plan.get("contents"), dict) or plan.get("fingerprint") != _digest(plan["contents"]):
        raise ValueError("A complete deterministic replacement plan is required")
    contents = plan["contents"]
    for name in ("run_id", "destination", "dataset", "refresh_scope", "expected_current_publication_fingerprint"):
        if contents.get(name) != manifest[name]:
            raise ValueError("A complete deterministic replacement plan is required")
    actions = contents.get("actions", [])
    keys = [(a.get("dataset"), a.get("start_date")) for a in actions]
    selected = set(manifest["start_dates"])
    if (len(keys) != len(set(keys)) or not {(manifest["dataset"], day) for day in selected} <= set(keys)
            or any(dataset != manifest["dataset"] for dataset, _ in keys)):
        raise ValueError("A complete deterministic replacement plan is required")
    for action in actions:
        day = action["start_date"]
        if day in selected:
            batch = manifest["batches"][day]
            expected_action = "REPLACE" if batch["row_count"] else ("REMOVE" if action.get("previous_partition") else "EMPTY")
            if action.get("action") != expected_action or action.get("counts") != batch["counts"]:
                raise ValueError("Conflicting replacement action")
        elif manifest["refresh_scope"] != "full" or action.get("action") != "REMOVE":
            raise ValueError("Action escapes refresh scope")


def transition_manifest(manifest, state, *, current_source_fingerprint=None, error=None, receipt_loader=None):
    """Local states only. PLANNED never means PUBLISHING or PUBLISHED."""
    _check_manifest(manifest)
    allowed = {"CREATED": {"STAGING", "FAILED"}, "STAGING": {"READY_TO_PLAN", "FAILED"},
               "READY_TO_PLAN": {"PLANNED", "FAILED"}, "PLANNED": {"FAILED"}, "FAILED": set()}
    if state not in allowed.get(manifest["state"], set()):
        raise ValueError("Forbidden local manifest transition")
    result = deepcopy(manifest)
    if state == "READY_TO_PLAN":
        result["totals"] = validate_run_readiness(result, current_source_fingerprint=current_source_fingerprint,
                                                 receipt_loader=receipt_loader)
    if state == "PLANNED":
        _check_complete_plan(result)
    if state == "FAILED":
        _evidence(error, "failure")
        result["error"] = error
    result["state"] = state
    return _bump(result)


def _published_inventory(records, destination):
    result, keys = [], set()
    root = DESTINATIONS[destination][1]
    for record in records:
        if set(record) != {"dataset", "start_date", "location", "counts", "publication_run_id"}:
            raise ValueError("Published partition inventory has unexpected/missing fields")
        dataset, day = record["dataset"], _day(record["start_date"]).isoformat()
        if dataset not in DATASETS or (destination == "production" and dataset == "sample"):
            raise ValueError("Published inventory destination/dataset mismatch")
        if record["location"] != f"{root}dataset={dataset}/start_date={day}/":
            raise ValueError("Published location crosses destination or is not canonical")
        if (dataset, day) in keys:
            raise ValueError("Duplicate/conflicting published partition key")
        _token(record["publication_run_id"])
        keys.add((dataset, day))
        result.append(dict(record, counts=_counts(record["counts"])))
    return sorted(result, key=lambda r: (r["dataset"], r["start_date"]))


def _publication_fingerprint(records, destination, current_publication):
    partitions = _published_inventory(records, destination)
    if partitions and current_publication is None:
        raise ValueError("Existing partitions require a previous-publication identity for recovery")
    return _digest(dict(destination=destination, publication=_previous(current_publication), partitions=partitions))


def plan_partition_replacement(manifest, published_partitions, *, current_publication,
                               current_source_fingerprint):
    """Plan actions only. Retry identical inputs returns the same plan/revision.

    Canonical fixed partition locations match the current project contract. A
    future versioned-location publisher must explicitly extend this inventory
    contract. No file copies, deletion, registration or publication occur here.
    """
    _check_manifest(manifest)
    if manifest["state"] not in ("READY_TO_PLAN", "PLANNED"):
        raise ValueError("Manifest must be READY_TO_PLAN")
    readiness = validate_run_readiness(manifest, current_source_fingerprint=current_source_fingerprint)
    inventory = _published_inventory(published_partitions, manifest["destination"])
    fingerprint = _publication_fingerprint(inventory, manifest["destination"], current_publication)
    if (fingerprint != manifest["expected_current_publication_fingerprint"]
            or _previous(current_publication) != manifest["previous_publication"]):
        raise ValueError("Stale publication plan: current publication changed")
    dataset = manifest["dataset"]
    old = {r["start_date"]: r for r in inventory if r["dataset"] == dataset}
    selected = set(manifest["start_dates"])
    actions = []
    for day in sorted(selected):
        batch = manifest["batches"][day]
        action = "REPLACE" if batch["row_count"] else ("REMOVE" if day in old else "EMPTY")
        actions.append(dict(action=action, dataset=dataset, start_date=day,
            reason="selected_nonempty" if batch["row_count"] else "selected_zero_output",
            target_location=f"{manifest['output_root']}dataset={dataset}/start_date={day}/",
            previous_partition=deepcopy(old.get(day)), staging_attempt=batch["attempt_path"],
            receipt_fingerprints=batch["receipt_fingerprints"], counts=batch["counts"]))
    if manifest["refresh_scope"] == "full":
        for day in sorted(set(old) - selected):
            actions.append(dict(action="REMOVE", dataset=dataset, start_date=day, reason="stale_outside_new_coverage",
                target_location=old[day]["location"], previous_partition=deepcopy(old[day]),
                staging_attempt=None, counts={"2": 0, "3": 0}))
    actions.sort(key=lambda r: (r["dataset"], r["start_date"]))
    action_keys = {(r["dataset"], r["start_date"]) for r in actions}
    if len(action_keys) != len(actions):
        raise ValueError("Duplicate/conflicting replacement actions")
    contents = dict(run_id=manifest["run_id"], destination=manifest["destination"], dataset=dataset,
        refresh_scope=manifest["refresh_scope"], expected_current_publication_fingerprint=fingerprint,
        previous_publication=manifest["previous_publication"], source_inventory_fingerprint=current_source_fingerprint,
        builder=manifest["builder"], rule_version=manifest["rule_version"], timezone=manifest["timezone"],
        source_coverage=manifest["source_coverage"], repair=manifest["repair"],
        identity_evidence_fingerprint=_digest(dict(report=manifest["identity_check"], coverage=manifest["coverage_evidence"])),
        actions=actions, preserved_partitions=[r for r in inventory if (r["dataset"], r["start_date"]) not in action_keys])
    plan = dict(contents=contents, fingerprint=_digest(contents))
    if manifest["state"] == "PLANNED":
        if manifest["plan"] != plan:
            raise ValueError("An existing plan cannot be silently replaced")
        return deepcopy(manifest)
    result = deepcopy(manifest)
    result["plan"], result["totals"] = plan, readiness
    return transition_manifest(result, "PLANNED")


def write_manifest_local(manifest, metadata_root, *, expected_revision=None):
    """Atomic JSON replacement under a dedicated metadata root; retain temp failures.

    Call under the local table-owner lock for cooperative concurrency. Revision
    checks detect stale callers, but are not an atomic distributed compare/swap.
    """
    _check_manifest(manifest)
    if manifest["state"] not in {"CREATED", "STAGING", "READY_TO_PLAN", "PLANNED", "FAILED"}:
        raise ValueError("Only local planning states can be written")
    if manifest["state"] == "PLANNED":
        _check_complete_plan(manifest)
    root = _local(metadata_root)
    if any(part in {"bq2_sequences", "bq2_sequences_dev"} for part in root.parts):
        raise ValueError("Manifest metadata must be outside table data roots")
    _token(manifest["run_id"])
    if manifest["destination"] not in DESTINATIONS:
        raise ValueError("Invalid manifest destination")
    target = _local(root / manifest["destination"] / manifest["run_id"] / "manifest.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        old = json.loads(target.read_text(encoding="utf-8"))
        if (expected_revision is None or old.get("revision") != expected_revision
                or old.get("run_id") != manifest["run_id"] or old.get("destination") != manifest["destination"]
                or manifest["revision"] <= expected_revision):
            raise ValueError("Stale or conflicting manifest update")
    elif expected_revision is not None:
        raise ValueError("Expected previous manifest is missing")
    temporary = target.with_name("manifest." + uuid4().hex + ".tmp")
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(manifest, handle, sort_keys=True, separators=(",", ":"), allow_nan=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)
    return target


@contextmanager
def publication_lock_local(lock_root, destination, run_id):
    """Exclusive owner per destination TABLE, not dataset; never steal stale locks.

    Only cooperative local processes are coordinated. This is NOT an AWS lock,
    does not pause Athena, and cannot replace reader/retention coordination.
    """
    if destination not in DESTINATIONS:
        raise ValueError("Invalid lock destination")
    _token(run_id)
    root = _local(lock_root)
    root.mkdir(parents=True, exist_ok=True)
    path = _local(root / f"{destination}.lock")
    owner = dict(run_id=run_id, token=uuid4().hex, table=DESTINATIONS[destination][0], acquired_at=_now())
    with path.open("x", encoding="utf-8") as handle:
        json.dump(owner, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    identity = (path.stat().st_dev, path.stat().st_ino)
    try:
        yield deepcopy(owner)
    finally:
        import sys
        active = sys.exc_info()[1]
        try:
            _local(path)
            current = json.loads(path.read_text(encoding="utf-8"))
            if current != owner or (path.stat().st_dev, path.stat().st_ino) != identity:
                raise ValueError("Publication lock ownership changed; lock preserved")
            path.unlink()
        except Exception as exc:
            if active is not None:
                active.add_note(f"Lock release failed; manual recovery required: {exc}")
            else:
                raise
