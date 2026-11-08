"""Check downloaded run/partition evidence locally; not live SQL acceptance."""
import argparse
from datetime import date, timedelta
import json
import hashlib
from pathlib import Path
import re

from render_acceptance import SPECS

ROOT = "s3://is459-g2t1-aml-processed-bucket/bq2_sequences/"
FINGERPRINTS = {"source_fingerprint", "table_fingerprint", "partitions_fingerprint"}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def dates(dataset):
    return [(date(2022, 9, 1) + timedelta(days=i)).isoformat()
            for i in range(SPECS[dataset]["days"])]


def counts(value):
    require(isinstance(value, dict) and set(value) == {"2", "3"}, "Explicit pair/triple counts required")
    require(all(type(n) is int and n >= 0 for n in value.values()), "Invalid sequence counts")
    return value


def review(manifest, publication, partitions):
    dataset = manifest.get("dataset")
    require(dataset in SPECS, "Only full HI/LI evidence is allowed")
    spec, days = SPECS[dataset], dates(dataset)
    require(manifest.get("manifest_kind") == "full_sequence_generation_handoff"
            and manifest.get("state") == "STAGED_VALIDATED"
            and manifest.get("published") is False
            and manifest.get("catalog_updated") is False
            and manifest.get("publication_required") is True, "Incomplete staging handoff")
    require(manifest.get("rule_version") == "v1", "Wrong rule version")
    require(manifest.get("owner_dates") == days and manifest.get("source_dates") == days,
            "Incomplete owner/source dates")
    snapshot = manifest.get("source", {})
    require(isinstance(snapshot, dict) and set(snapshot) == FINGERPRINTS
            and all(isinstance(v, str) and re.fullmatch(r"[0-9a-f]{64}", v) for v in snapshot.values()),
            "Three exact source/Catalog fingerprints required")
    require(manifest.get("source_after") == snapshot, "Source snapshot changed")
    profile = manifest.get("source_validation", {})
    require(profile.get("row_count") == spec["rows"] and profile.get("laundering_rows") == spec["positive"]
            and profile.get("identity_check_passed") is True
            and profile.get("full_selected_snapshot_checked") is True
            and profile.get("checked_dates") == days and profile.get("source") == snapshot,
            "Source profile does not match the selected snapshot/baseline")
    totals = counts(manifest.get("counts"))
    require(manifest.get("row_count") == sum(totals.values()), "Run totals disagree")
    batches = manifest.get("batches", [])
    require(len(batches) == len(days) and sorted(b.get("start_date", "") for b in batches) == days,
            "Every owner date needs exactly one selected batch, including empty dates")
    for batch in batches:
        counts(batch.get("counts"))
    require(all(sum(b["counts"][k] for b in batches) == totals[k] for k in ("2", "3")),
            "Batch/run counts disagree")
    require(publication.get("manifest_kind") == "full_sequence_publication"
            and publication.get("state") == "PUBLISHED"
            and all(publication.get(k) is True for k in ("published", "catalog_updated", "verified")),
            "Verified PUBLISHED receipt required")
    binding = publication.get("binding", {})
    identity = {k: manifest.get(k) for k in ("dataset", "run_id", "attempt_id", "owner_token")}
    require(all(isinstance(v, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", v)
                for k, v in identity.items() if k != "dataset"), "Invalid run identity")
    version = ROOT + "dataset=" + dataset + "/versions/" + identity["run_id"] + "/" + identity["attempt_id"] + "/"
    require(binding.get("identity") == identity and binding.get("source") == snapshot
            and binding.get("generation_fingerprint") == digest(manifest)
            and binding.get("counts") == totals and binding.get("row_count") == manifest["row_count"]
            and binding.get("version_root") == version, "Publication is bound to a different run/source")
    require(isinstance(partitions, list), "Supply a complete paginated GetPartitions export list")
    require(all(isinstance(p, dict) and isinstance(p.get("Values"), list)
                and len(p["Values"]) == 2 for p in partitions), "Malformed Catalog partition keys")
    selected = [p for p in partitions if p.get("Values", [None])[0] == dataset]
    require(len(selected) == len(days)
            and sorted(p.get("Values", []) for p in selected) == [[dataset, d] for d in days],
            "Missing, extra, duplicate or incorrectly ordered Catalog dates")
    for part in selected:
        day = part["Values"][1]
        require(part.get("StorageDescriptor", {}).get("Location") == version + "data/dataset=" + dataset + "/start_date=" + day + "/",
                "Mixed or unexpected published partition version")
    return {"status": "EVIDENCE_BINDING_PASSED", "dataset": dataset,
            "owner_dates": len(days), "counts": totals,
            "live_athena_acceptance": "NOT_ESTABLISHED_BY_THIS_REVIEW"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "publication", "partitions"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    payloads = [json.loads(getattr(args, k).read_text(encoding="utf-8-sig"))
                for k in ("manifest", "publication", "partitions")]
    print(json.dumps(review(*payloads), indent=2))


if __name__ == "__main__":
    main()
