"""Increment 3A: PURE development plans and SIMULATED operation journals.

No AWS clients, IO, copying, Catalog mutations, rollback or publication success.
Inputs are supplied snapshots/attestations, not independently obtained proof.
READY_TO_PLAN qualifies after fresh shared readiness checks; local PLANNED also
requires its original complete plan, but that fixed-location plan is NOT executed.
Local staging references can be planned offline but block later cloud execution.
The returned journal is JSON-serializable, NOT durably stored by this module.
Later adapters must persist intent conditionally BEFORE performing any operation.
"""

from copy import deepcopy
from pathlib import PurePosixPath, PureWindowsPath

from sequence_manifest import (
    _token, _hash, _day, _dates, _counts, _digest, _previous,
    _check_manifest, _check_complete_plan, validate_run_readiness,
)

DATABASE = "is459_g2t1_aml"
TABLE = "bq2_sequences_dev"
BUCKET = "is459-g2t1-aml-processed-bucket"
ROOT = "s3://" + BUCKET + "/"
TABLE_ROOT = ROOT + "bq2_sequences_dev/"
VERSION_ROOT = ROOT + "bq2_sequences_dev_versions/"
STAGING_ROOT = ROOT + "bq2_sequences_staging/"

# Catalog definition only; no analytical columns are added by versioning/journals.
DATA_COLUMNS = (
    ("sequence_id", "string"), ("rule_version", "string"), ("sequence_length", "int"),
    ("txn_ids", "array<bigint>"), ("txn_timestamps", "array<timestamp>"),
    ("payment_methods", "array<string>"), ("method_sequence", "string"),
    ("payment_currencies", "array<string>"), ("receiving_currencies", "array<string>"),
    ("payment_currency_sequence", "string"), ("receiving_currency_sequence", "string"),
    ("txn_laundering_labels", "array<int>"), ("laundering_count", "int"),
    ("any_laundering", "int"), ("all_laundering", "int"), ("final_txn_id", "bigint"),
    ("final_is_laundering", "int"), ("gap_seconds", "array<bigint>"), ("duration_seconds", "bigint"),
)


def _reference(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Independent evidence reference required")


def _safe_s3(uri):
    if (not isinstance(uri, str) or not uri.startswith(ROOT)
            or any(c in uri for c in ("%", "\\", "?", "#"))
            or any(p in ("", ".", "..") for p in uri[len(ROOT):].rstrip("/").split("/"))):
        raise ValueError("Unsafe bucket/path")
    return uri


def _partition_location(location, dataset, day):
    _safe_s3(location)
    suffix = f"dataset={dataset}/start_date={day}/"
    if location == TABLE_ROOT + suffix:
        return
    if location.startswith(VERSION_ROOT) and location.endswith("/" + suffix):
        publication = location[len(VERSION_ROOT):].removesuffix("/" + suffix)
        _token(publication)
        return
    raise ValueError("Partition location escapes approved development locations")


def _columns(value):
    if not isinstance(value, list) or [(c.get("Name"), c.get("Type")) for c in value] != list(DATA_COLUMNS):
        raise ValueError("Catalog data columns must match approved 19 physical columns")


def _table(value):
    if (not isinstance(value, dict) or value.get("DatabaseName") != DATABASE or value.get("Name") != TABLE
            or value.get("StorageDescriptor", {}).get("Location") != TABLE_ROOT
            or value.get("PartitionKeys") != [dict(Name="dataset", Type="string"), dict(Name="start_date", Type="string")]):
        raise ValueError("Only the exact development table and string partition keys are supported")
    _columns(value["StorageDescriptor"].get("Columns"))
    if str(value.get("Parameters", {}).get("projection.enabled", "false")).lower() != "false":
        raise ValueError("Partition projection must be disabled for explicit-location publication")
    _digest(value)  # Require JSON-compatible snapshots, not live client objects.


def _partitions(values):
    if not isinstance(values, list):
        raise ValueError("Partition definitions must be a list")
    result, keys = [], set()
    for value in values:
        definition = deepcopy(value)
        pair = definition.get("Values", [])
        if not isinstance(pair, list) or len(pair) != 2 or pair[0] not in ("sample", "HI-Medium", "LI-Medium"):
            raise ValueError("Invalid partition identity")
        dataset, day = pair
        _day(day)
        key = (dataset, day)
        if key in keys:
            raise ValueError("Duplicate partition identity")
        keys.add(key)
        storage = definition.get("StorageDescriptor", {})
        _columns(storage.get("Columns"))
        _partition_location(storage.get("Location"), dataset, day)
        _digest(definition)
        result.append(definition)
    return sorted(result, key=lambda p: tuple(p["Values"]))


def _staging_path(path):
    if not isinstance(path, str):
        raise ValueError("Staging path required")
    if path.startswith("s3://"):
        _safe_s3(path)
        parts = path.removeprefix(STAGING_ROOT).rstrip("/").split("/")
        if not path.startswith(STAGING_ROOT) or len(parts) != 2 or not path.endswith("/"):
            raise ValueError("Expected canonical staging run/attempt prefix")
        for part in parts:
            _token(part)
        return "s3"
    if "://" in path or path.startswith(("//", "\\\\")):
        raise ValueError("Unsafe staging path")
    parsed = PureWindowsPath(path) if "\\" in path or (len(path) > 1 and path[1] == ":") else PurePosixPath(path)
    if not parsed.is_absolute() or any(part in (".", "..") for part in parsed.parts):
        raise ValueError("Local staging path must be absolute and safe")
    return "local"


def plan_development_publication(run_manifest, publication_id, selected_dates, *,
                                  frozen_run_fingerprint, table, partitions,
                                  staging_evidence, recovery_evidence, current_publication,
                                  current_source_fingerprint, owner, maintenance_ack,
                                  database=DATABASE, table_name=TABLE, bucket=BUCKET,
                                  destination="development", dataset="sample", scope="partial"):
    """Return a deterministic plan from injected snapshots, never fetch evidence.

    staging_evidence[date] = {receipt: normalized shared-manifest receipt evidence,
      verified: True, verification_reference: str, inventory_fingerprint: sha256}.
    recovery_evidence[date] binds prior definition/inventory/counts/publication.
    Dates outside the selected set are preserved, including other datasets.
    """
    if (database, table_name, bucket, destination, dataset, scope) != (
            DATABASE, TABLE, BUCKET, "development", "sample", "partial"):
        raise ValueError("Only partial sample/development publication planning is allowed")
    _token(publication_id)
    _check_manifest(run_manifest)
    if run_manifest["dataset"] != "sample" or run_manifest["destination"] != "development":
        raise ValueError("Run manifest must be sample/development")
    state = run_manifest.get("state")
    if state not in ("READY_TO_PLAN", "PLANNED"):
        raise ValueError("Run must be READY_TO_PLAN or a valid local PLANNED manifest")
    if state == "PLANNED":
        _check_complete_plan(run_manifest)
    elif run_manifest.get("plan") is not None:
        raise ValueError("READY_TO_PLAN cannot contain a replacement plan")
    _hash(frozen_run_fingerprint)
    if _digest(run_manifest) != frozen_run_fingerprint:
        raise ValueError("Frozen run manifest changed")
    if run_manifest["timezone"] != "UTC":
        raise ValueError("UTC-only builder requires independently verified upstream semantics")
    _reference(run_manifest["timezone_evidence"])
    dates = _dates(selected_dates)
    if not dates or not set(dates) <= set(run_manifest["start_dates"]):
        raise ValueError("Selected dates must be nonempty and covered by the run")
    _table(table)
    inventory = _partitions(partitions)
    previous = _previous(current_publication)
    if previous != run_manifest["previous_publication"]:
        raise ValueError("Previous publication changed")
    if not isinstance(owner, dict) or owner.get("table") != DATABASE + "." + TABLE:
        raise ValueError("A table-wide publication owner is required")
    _token(owner.get("token"))
    _reference(owner.get("evidence_reference"))
    if (not isinstance(maintenance_ack, dict) or maintenance_ack.get("acknowledged") is not True
            or maintenance_ack.get("active_queries_drained") is not True
            or maintenance_ack.get("table") != DATABASE + "." + TABLE
            or maintenance_ack.get("publication_id") != publication_id):
        raise ValueError("Role 6/Athena maintenance-window acknowledgment is required")
    _reference(maintenance_ack.get("acknowledged_by"))
    _reference(maintenance_ack.get("reference"))
    if not isinstance(staging_evidence, dict) or not isinstance(recovery_evidence, dict):
        raise ValueError("Injected staging/recovery evidence maps required")
    if set(staging_evidence) != set(run_manifest["start_dates"]):
        raise ValueError("Staging evidence must cover every date in the frozen run")
    for day, evidence in staging_evidence.items():
        if not isinstance(evidence, dict) or evidence.get("receipt", {}).get("status", {}).get("start_date") != day:
            raise ValueError("Staging evidence date mismatch")
    def load(path):
        matches = [e for e in staging_evidence.values() if e.get("receipt", {}).get("attempt_path") == path]
        if len(matches) != 1:
            raise ValueError("Missing/ambiguous staging evidence")
        evidence = matches[0]
        if evidence.get("verified") is not True:
            raise ValueError("Staging Parquet/inventory must be independently verified")
        _reference(evidence.get("verification_reference"))
        _hash(evidence.get("inventory_fingerprint"))
        receipt = evidence["receipt"]
        kind = _staging_path(path)
        if kind == "s3" and path[len(STAGING_ROOT):].split("/")[0] != run_manifest["run_id"]:
            raise ValueError("Staging URI belongs to another run")
        for name in ("status", "validation"):
            expected = path.rstrip("/") + "/metadata/" + name + ".json"
            if receipt.get(name + "_receipt") != expected:
                raise ValueError("Unsafe/mismatched staging receipt path")
        if receipt["status"].get("state") != "VALIDATED":
            raise ValueError("Staging attempt must be VALIDATED")
        source = receipt["status"].get("source_fingerprint", current_source_fingerprint)
        if source != current_source_fingerprint:
            raise ValueError("Staging source inventory changed")
        if not {"status", "validation"} <= set(receipt["fingerprints"]):
            raise ValueError("Missing staging receipt fingerprints")
        for fingerprint in receipt["fingerprints"].values():
            _hash(fingerprint)
        return receipt
    readiness = validate_run_readiness(run_manifest, current_source_fingerprint=current_source_fingerprint,
                                       receipt_loader=load)
    for name in ("counts", "row_count", "source_fingerprint", "receipts_verified"):
        if not isinstance(run_manifest.get("totals"), dict) or run_manifest["totals"].get(name) != readiness[name]:
            raise ValueError("Frozen run readiness totals are inconsistent")
    old = {p["Values"][1]: p for p in inventory if p["Values"][0] == "sample"}
    actions, local = [], []
    for index, day in enumerate(dates, 1):
        batch = run_manifest["batches"][day]
        counts = _counts(batch["counts"])
        before = old.get(day)
        recovery = None
        if before is not None:
            recovery = recovery_evidence.get(day)
            if not isinstance(recovery, dict) or recovery.get("verified") is not True or previous is None:
                raise ValueError("Existing selected partition requires verified previous-publication recovery evidence")
            if recovery.get("partition_fingerprint") != _digest(before):
                raise ValueError("Recovery evidence does not match old partition definition")
            _hash(recovery.get("inventory_fingerprint"))
            _counts(recovery.get("counts"))
            if _previous(recovery.get("publication_reference")) is None:
                raise ValueError("Recovery must identify the prior publication")
            _reference(recovery.get("verification_reference"))
        outcome = "REPLACE" if sum(counts.values()) else ("REMOVE" if before is not None else "EMPTY")
        after = None
        location = VERSION_ROOT + publication_id + "/dataset=sample/start_date=" + day + "/"
        if outcome == "REPLACE":
            storage = deepcopy(table["StorageDescriptor"])
            storage["Location"] = location
            after = dict(Values=["sample", day], StorageDescriptor=storage,
                         Parameters={"role5_publication_id": publication_id, "role5_rule_version": "v1"})
        kind = _staging_path(batch["attempt_path"])
        if kind == "local":
            local.append(day)
        actions.append(dict(operation_id=f"op-{index:04d}", dataset="sample", start_date=day, outcome=outcome,
            catalog_action=("UPDATE" if before else "CREATE") if outcome == "REPLACE" else ("DELETE" if before else "NONE"),
            expected_old=deepcopy(before), intended_new=after, counts=counts, staging=deepcopy(batch),
            staging_inventory_fingerprint=staging_evidence[day]["inventory_fingerprint"],
            recovery=deepcopy(recovery)))
    contents = dict(format_version=1, publication_id=publication_id, database=DATABASE, table=TABLE,
        bucket=BUCKET, dataset="sample", scope="partial", version_root=VERSION_ROOT,
        run_reference=dict(run_id=run_manifest["run_id"], state=state, fingerprint=frozen_run_fingerprint),
        source_fingerprint=current_source_fingerprint, previous_publication=previous,
        expected_table=deepcopy(table), expected_partitions=inventory,
        catalog_fingerprint=_digest(dict(table=table, partitions=inventory)), selected_dates=dates,
        actions=actions, preserved_partitions=[p for p in inventory if p["Values"][0] != "sample"
                                              or p["Values"][1] not in dates],
        owner=deepcopy(owner), maintenance_ack=deepcopy(maintenance_ack),
        execution_blockers=["Local staging must be materialized/validated in S3 and replanned: " + day for day in local],
        controls=dict(atomic_multi_partition=False, owner_and_reader_pause="cooperative; must be rechecked at execution",
                      recovery="prior files must remain unchanged; references are not backups",
                      evidence="supplied snapshots only; no AWS or source verification performed"))
    return dict(contents=contents, fingerprint=_digest(contents))


def assert_publication_plan_current(plan, run_manifest, selected_dates, **snapshots):
    """Rebuild from freshly supplied observations; any bound evidence change stops."""
    _validate_plan(plan)
    current = plan_development_publication(run_manifest, plan["contents"]["publication_id"], selected_dates, **snapshots)
    if current != plan:
        raise ValueError("Stale publication plan: bound evidence changed")


def _validate_plan(plan):
    if not isinstance(plan, dict) or not isinstance(plan.get("contents"), dict) or plan.get("fingerprint") != _digest(plan["contents"]):
        raise ValueError("Invalid publication plan fingerprint")
    c = plan["contents"]
    if (c.get("database"), c.get("table"), c.get("bucket"), c.get("dataset"), c.get("scope"), c.get("version_root")) != (
            DATABASE, TABLE, BUCKET, "sample", "partial", VERSION_ROOT):
        raise ValueError("Plan destination isolation violated")


def classify_operation_state(operation, observed_definition):
    """Compare complete supplied Catalog definitions; no API calls or mutations.

    EMPTY has identical old/new absence, so returns ALREADY_EMPTY explicitly.
    Observation comparison is semantic evidence only, not an execution receipt.
    """
    before, after = operation["expected_old"], operation["intended_new"]
    if before is None and after is None and observed_definition is None:
        return "ALREADY_EMPTY"
    if observed_definition == before:
        return "EXPECTED_OLD"
    if observed_definition == after:
        return "INTENDED_NEW"
    return "CONFLICT"


def create_publication_manifest(plan):
    """Create an ordered journal suitable for later durable JSON storage."""
    _validate_plan(plan)
    entries = [dict(sequence=i, operation_id=action["operation_id"], intent=deepcopy(action),
                    status="PENDING", observations=[]) for i, action in enumerate(plan["contents"]["actions"], 1)]
    return dict(format_version=1, publication_id=plan["contents"]["publication_id"],
                plan=deepcopy(plan), state="PLANNED", revision=0, journal=entries,
                mode="PLANNING_ONLY", publication_success_allowed=False)


def _validate_journal(manifest):
    _validate_plan(manifest["plan"])
    if (manifest.get("mode") != "PLANNING_ONLY" or manifest.get("publication_success_allowed") is not False
            or manifest.get("state") not in ("PLANNED", "SIMULATING", "RECOVERY_REQUIRED")
            or type(manifest.get("revision")) is not int or manifest["revision"] < 0
            or manifest.get("publication_id") != manifest["plan"]["contents"]["publication_id"]):
        raise ValueError("Invalid planning-only journal state/identity")
    actions = manifest["plan"]["contents"]["actions"]
    journal = manifest.get("journal", [])
    if len(journal) != len(actions):
        raise ValueError("Journal does not match the frozen plan")
    statuses = {"PENDING", "INTENT_RECORDED", "OBSERVED_OLD_SIMULATED", "OBSERVED_NEW_SIMULATED", "CONFLICT"}
    for i, (entry, action) in enumerate(zip(journal, actions), 1):
        if (entry.get("sequence") != i or entry.get("operation_id") != action["operation_id"]
                or entry.get("intent") != action or entry.get("status") not in statuses):
            raise ValueError("Journal intent/order changed")


def transition_publication_manifest(manifest, state):
    """Only planning/recovery-required states are reachable in Increment 3A.

    PREPARING/PREPARED/PUBLISHING/VERIFYING/PUBLISHED and rollback execution
    require future adapters/independent receipts; even mocked claims are refused.
    """
    _validate_journal(manifest)
    allowed = {"PLANNED": {"RECOVERY_REQUIRED"}, "SIMULATING": {"RECOVERY_REQUIRED"}}
    if state not in allowed.get(manifest.get("state"), set()):
        raise ValueError("Execution/publication success transitions are unavailable in planning-only mode")
    result = deepcopy(manifest)
    result.update(state=state, revision=manifest["revision"] + 1)
    return result


def record_operation_intent(manifest, operation_id):
    """Return a new journal with intent recorded, without executing anything.

    A future durable adapter must save this returned revision before mutation.
    Simulation cannot establish PUBLISHED or actual Catalog success.
    """
    _validate_journal(manifest)
    if manifest["state"] not in ("PLANNED", "SIMULATING"):
        raise ValueError("Journal is not available for planning simulation")
    result = deepcopy(manifest)
    entry = next((e for e in result["journal"] if e["operation_id"] == operation_id), None)
    if entry is None or entry["status"] != "PENDING":
        raise ValueError("Unknown operation or intent already recorded")
    if any(e["status"] != "OBSERVED_NEW_SIMULATED" for e in result["journal"][:entry["sequence"] - 1]):
        raise ValueError("Earlier operation observations must be recorded first")
    entry["status"] = "INTENT_RECORDED"
    result.update(state="SIMULATING", revision=manifest["revision"] + 1)
    return result


def record_operation_observation(manifest, operation_id, observed_definition, *, reference):
    """Append an ordered simulated observation; conflicting states block progress."""
    _reference(reference)
    _validate_journal(manifest)
    if manifest["state"] != "SIMULATING":
        raise ValueError("Intent must be recorded before observing an operation")
    result = deepcopy(manifest)
    entry = next((e for e in result["journal"] if e["operation_id"] == operation_id), None)
    if entry is None or entry["status"] not in ("INTENT_RECORDED", "OBSERVED_OLD_SIMULATED"):
        raise ValueError("Operation has no outstanding intent")
    classification = classify_operation_state(entry["intent"], observed_definition)
    entry["observations"].append(dict(sequence=len(entry["observations"]) + 1, classification=classification,
        definition=deepcopy(observed_definition), reference=reference, simulated=True))
    entry["status"] = {"EXPECTED_OLD": "OBSERVED_OLD_SIMULATED", "INTENDED_NEW": "OBSERVED_NEW_SIMULATED",
                       "ALREADY_EMPTY": "OBSERVED_NEW_SIMULATED", "CONFLICT": "CONFLICT"}[classification]
    result["revision"] += 1
    if classification == "CONFLICT":
        result["state"] = "RECOVERY_REQUIRED"
    return result
