"""Immutable shared chunks; one conditional progress update commits a root group.

Unreferenced group/chunk receipts are diagnostic orphans, never recovered output.
No publication or Catalog API exists here.
"""
from copy import deepcopy
from sequence_owner_date import OwnerDateStager
from sequence_s3 import _get, _put
from sequence_manifest import _digest
from sequence_full_source import inventory_objects
from sequence_source import fingerprint_source_inventory


class BatchedDateStager(OwnerDateStager):
    def begin_date(self, day, root_chunks, owner_rows):
        # Do not apply the legacy single-root chunk reconstruction to shared files.
        completed = self.completed_roots
        self.completed_roots = {}
        try:
            super().begin_date(day, root_chunks, owner_rows)
        finally:
            self.completed_roots = completed
        date = self.dates[day]
        groups = {}
        for key, record in completed.items():
            uri = record.get("group_uri", "")
            prefix = date["work"] + "invocations/"
            if not uri.startswith(prefix) or not uri.endswith("/group.json"):
                raise ValueError("Unsafe group receipt")
            if uri not in groups:
                group, _, _ = _get(self.client, uri)
                self._validate_group(group, day, root_chunks)
                if _digest(group) != record.get("group_fingerprint"):
                    raise ValueError("Changed group receipt")
                groups[uri] = group
                for cid, chunk in group["chunks"].items():
                    if cid in date["chunks"]:
                        raise ValueError("Shared chunk referenced twice")
                    self._verify_chunk(day, cid, chunk)
                    date["chunks"][cid] = chunk
            group = groups[uri]
            if _digest(group) != record.get("group_fingerprint") or key not in group["roots"]:
                raise ValueError("Group membership changed")
            root = group["roots"][key]
            observed, _, _ = _get(self.client, root["receipt_uri"])
            if observed != root["receipt"]:
                raise ValueError("Root evidence changed")
            date["roots"][int(key)] = observed
            date["root_uris"][int(key)] = root["receipt_uri"]
        for uri, group in groups.items():
            referenced = {k for k, r in completed.items() if r.get("group_uri") == uri}
            if referenced != set(group["roots"]):
                raise ValueError("Partially committed group")

    def _verify_chunk(self, day, cid, chunk):
        date = self.dates[day]
        uri = chunk.get("receipt_uri", "")
        if not uri.startswith(date["work"] + "invocations/") or not uri.endswith("/" + cid + "/validation.json"):
            raise ValueError("Unsafe shared chunk")
        actual, _, _ = _get(self.client, uri)
        status, _, _ = _get(self.client, uri.removesuffix("validation.json") + "status.json")
        if actual != {k: v for k, v in chunk.items() if k != "receipt_uri"}:
            raise ValueError("Changed shared chunk receipt")
        if (chunk.get("chunk_id")!=cid or status.get("chunk_id")!=cid or status.get("root")!=chunk.get("root")
                or status.get("state") != "VALIDATED" or status.get("owner_token") != self.config["owner_token"]):
            raise ValueError("Unvalidated shared chunk")
        files = inventory_objects(self.client, uri.removesuffix("validation.json") + "data/")
        if files != chunk["files"] or fingerprint_source_inventory(files) != chunk["inventory_fingerprint"]:
            raise ValueError("Changed shared chunk files")

    def _validate_group(self, group, day, root_chunks):
        if (group.get("kind") != "validated_root_group" or group.get("source") != self.manifest["source"]
                or group.get("identity") != self.manifest["identity"] or group.get("start_date") != day
                or group.get("owner_token") != self.config["owner_token"]):
            raise ValueError("Group identity mismatch")
        roots = group.get("roots", {})
        if not roots or any(str(int(k)) != k or not 0 <= int(k) < root_chunks for k in roots):
            raise ValueError("Invalid group roots")
        counts = {str(k): sum(r["receipt"]["counts"][str(k)] for r in roots.values()) for k in (2, 3)}
        chunks = group["chunks"]
        if any(str(c.get("root")) not in roots or set(c.get("counts",{}))!={"2","3"}
                or any(type(v) is not int or v<0 for v in c["counts"].values()) for c in chunks.values()):
            raise ValueError("Invalid shared chunk ownership/counts")
        if counts != {str(k): sum(c["counts"][str(k)] for c in chunks.values()) for k in (2, 3)}:
            raise ValueError("Group output counts disagree")
        for key, root in roots.items():
            receipt = root["receipt"]
            if (receipt.get("root") != int(key) or receipt.get("state") != "COMPLETE"
                    or type(receipt.get("input_rows")) is not int or receipt["input_rows"] < 0
                    or set(receipt.get("counts", {})) != {"2", "3"}
                    or any(type(v) is not int or v < 0 for v in receipt["counts"].values())
                    or not root["receipt_uri"].startswith(self.dates[day]["work"] + "invocations/")
                    or not root["receipt_uri"].endswith("/root-" + key + ".json")):
                raise ValueError("Invalid group root evidence")
        paths = [r["path"] for c in chunks.values() for r in c["files"]]
        if len(paths) != len(set(paths)):
            raise ValueError("Duplicate physical group file")

    def commit_group(self, day, result, *, verify_source):
        """Caller finishes owned-cache cleanup; recheck source before visibility."""
        if not callable(verify_source):raise ValueError("Explicit source reinspection required")
        if result.get("state")!="GENERATED_NOT_STAGED":raise ValueError("Completed kernel evidence required")
        date = self._owned_date(day)
        roots = result["roots"]
        if any(str(r) in self.manifest["root_progress"] for r in roots):
            raise ValueError("Group overlaps completed roots")
        chunks = {cid: date["chunks"][cid] for cid in result["chunks"]}
        base = date["work"] + "invocations/" + self.manifest["invocation_id"] + "/"
        records = {}
        for root in roots:
            receipt = dict(root=root, input_rows=result["input_rows"][root],
                counts={str(k): result["counts"][root][k] for k in (2, 3)}, state="COMPLETE")
            records[str(root)] = dict(receipt=receipt, receipt_uri=base + "root-" + str(root) + ".json")
        group = dict(kind="validated_root_group", start_date=day, identity=self.manifest["identity"],
            source=self.manifest["source"], owner_token=self.config["owner_token"], roots=records, chunks=chunks)
        self._validate_group(group, day, date["status"]["root_chunks"])
        for cid, chunk in chunks.items():
            self._verify_chunk(day, cid, chunk)
        for record in records.values():
            _put(self.client, record["receipt_uri"], record["receipt"])
        uri = base + "group.json"
        _put(self.client, uri, group)
        actual, _, _ = _get(self.client, uri)
        if actual != group:
            raise ValueError("Unconfirmed group receipt")
        verify_source()
        progress = deepcopy(self.manifest["root_progress"])
        for root, record in records.items():
            progress[root] = dict(counts=record["receipt"]["counts"], group_uri=uri, group_fingerprint=_digest(group))
        # Single CAS is the visibility boundary for ALL roots in this group.
        self.save(state="PARTIAL_CHUNKS", root_progress=progress)
        self.completed_roots = deepcopy(progress)
        for root, record in records.items():
            date["roots"][int(root)] = record["receipt"]
            date["root_uris"][int(root)] = record["receipt_uri"]
        return group
