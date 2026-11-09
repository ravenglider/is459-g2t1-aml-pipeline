"""Production HI/LI version publication, explicit Catalog switching and recovery.

Injected clients only. Table-wide S3 CAS ownership coordinates cooperating
writers. Glue partition updates have NO CAS or multi-partition transaction:
readers and out-of-band writers must remain paused until success/recovery.
Never delete old files, take over a lock, or silently retry a Catalog mutation.
"""
from copy import deepcopy
import base64
import hashlib
import json
import re
import time

from sequence_manifest import _digest, _now, _counts
from sequence_full_source import (
    BUCKET, dataset_spec, owner_dates, inspect_full_source, snapshot_identity, _serial,
)
from sequence_full_generation import validate_full_config
from sequence_full_staging import STAGING_ROOT
from sequence_s3 import _SparkParquetTransport
from sequence_staging import SEQUENCE_SCHEMA

DATA_COLUMNS = tuple((f.name,f.dataType.simpleString()) for f in SEQUENCE_SCHEMA.fields[:-2])

DATABASE = "is459_g2t1_aml"
TABLE = "bq2_sequences"
ROOT = f"s3://{BUCKET}/bq2_sequences/"
METADATA = f"s3://{BUCKET}/bq2_sequences_metadata/production/"
LOCK = METADATA + "table-lock.json"


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _key(uri):
    roots = (ROOT, METADATA, STAGING_ROOT)
    _require(isinstance(uri, str) and uri.startswith(roots)
        and not any(c in uri for c in ("%", "\\", "?", "#"))
        and all(p not in ("", ".", "..") for p in uri.split("/",3)[3].rstrip("/").split("/")),
        "Unsafe production publication URI")
    return uri.split("/",3)[3]


def _raw(value):
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False).encode("utf-8")


def _code(error):
    return str(getattr(error,"response",{}).get("Error",{}).get("Code",""))


class _Store:
    def __init__(self, client):
        self.client = client

    def get(self, uri, *, optional=False, **conditions):
        try:
            response = self.client.get_object(Bucket=BUCKET,Key=_key(uri),**conditions)
        except Exception as error:
            if optional and _code(error) in ("NoSuchKey","404","NotFound"):
                return None
            raise
        try:
            raw = response["Body"].read()
        finally:
            response["Body"].close()
        _require(isinstance(raw,bytes) and isinstance(response.get("ETag"),str),"Missing object/version token")
        return raw,response["ETag"]

    def json(self, uri, *, optional=False):
        value = self.get(uri,optional=optional)
        if value is None:
            return None
        raw,etag = value
        result = json.loads(raw)
        _require(isinstance(result,dict),"Object receipt required")
        return result,etag

    def put(self, uri, value, etag=None):
        _require(uri.startswith(METADATA),"Publication JSON restricted to production metadata")
        raw = _raw(value)
        condition = {"IfNoneMatch":"*"} if etag is None else {"IfMatch":etag}
        try:
            self.client.put_object(Bucket=BUCKET,Key=_key(uri),Body=raw,ContentType="application/json",
                ServerSideEncryption="AES256",**condition)
        except Exception as original:
            if _code(original) in ("412","409","PreconditionFailed","ConditionalRequestConflict"):
                raise ValueError("Conditional publication conflict; no takeover") from original
            try:
                observed = self.get(uri)
                if observed[0] == raw:
                    return observed[1]
            except Exception as inspection:
                original.add_note("Uncertain metadata outcome: "+type(inspection).__name__)
            raise
        observed = self.get(uri)
        _require(observed[0] == raw,"Unconfirmed publication metadata write")
        return observed[1]

    def inventory(self, prefix):
        _require(prefix.endswith("/"),"Inventory prefix must end in slash")
        key = _key(prefix)
        result,seen,tokens,token = [],set(),set(),None
        while True:
            args = dict(Bucket=BUCKET,Prefix=key)
            if token is not None:
                args["ContinuationToken"] = token
            page = self.client.list_objects_v2(**args)
            for item in page.get("Contents",[]):
                name = item.get("Key")
                _require(isinstance(name,str) and name.startswith(key) and name not in seen,"Malformed/duplicate publication inventory")
                seen.add(name)
                stamp = item["LastModified"]
                from datetime import timezone
                _require(stamp.tzinfo is not None and type(item["Size"]) is int and item["Size"] >= 0
                    and isinstance(item["ETag"],str),"Malformed inventory metadata")
                result.append(dict(path=f"s3://{BUCKET}/"+name,size=item["Size"],etag=item["ETag"],
                    modification_time=stamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")))
            if page.get("IsTruncated") is not True:
                break
            token = page.get("NextContinuationToken")
            _require(isinstance(token,str) and token and token not in tokens,"Invalid publication pagination")
            tokens.add(token)
        return sorted(result,key=lambda r:r["path"])


def _columns():
    return [dict(Name=n,Type=t) for n,t in DATA_COLUMNS]


def table_input():
    return dict(Name=TABLE,TableType="EXTERNAL_TABLE",Parameters={"EXTERNAL":"TRUE","classification":"parquet",
        "projection.enabled":"false"},PartitionKeys=[dict(Name="dataset",Type="string"),dict(Name="start_date",Type="string")],
        StorageDescriptor=dict(Columns=_columns(),Location=ROOT,
            InputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            OutputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            SerdeInfo=dict(SerializationLibrary="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")))


def _storage_view(storage):
    return dict(Columns=storage.get("Columns"),Location=storage.get("Location"),
        InputFormat=storage.get("InputFormat"),OutputFormat=storage.get("OutputFormat"),
        Serde=storage.get("SerdeInfo",{}).get("SerializationLibrary"))


def _validate_table(table):
    expected = table_input()
    _require(table.get("Name")==TABLE and table.get("TableType")=="EXTERNAL_TABLE"
        and table.get("PartitionKeys")==expected["PartitionKeys"]
        and _storage_view(table.get("StorageDescriptor",{}))==_storage_view(expected["StorageDescriptor"])
        and str(table.get("Parameters",{}).get("projection.enabled","false")).lower()=="false",
        "Incompatible production table; no schema/location changes allowed")


def _partition_view(partition):
    if partition is None:
        return None
    return dict(Values=partition["Values"],Storage=_storage_view(partition["StorageDescriptor"]),
                Parameters=partition.get("Parameters",{}))


def _plan_fingerprint(receipt):
    return _digest(dict(binding=receipt["binding"],previous_table=receipt["previous_table"],
        other_partitions=receipt["other_partitions"],actions=[{k:a[k] for k in
            ("start_date","before","after","previous_inventory")} for a in receipt["actions"]]))


def _catalog(catalog):
    try:
        table = catalog.get_table(DatabaseName=DATABASE,Name=TABLE)["Table"]
    except Exception as error:
        if _code(error)=="EntityNotFoundException":
            return None,[]
        raise
    _validate_table(table)
    partitions,tokens,token,seen = [],set(),None,set()
    while True:
        args=dict(DatabaseName=DATABASE,TableName=TABLE)
        if token is not None:
            args["NextToken"]=token
        page=catalog.get_partitions(**args)
        for part in page.get("Partitions",[]):
            values=part.get("Values")
            _require(isinstance(values,list) and len(values)==2 and values[0] in ("HI-Medium","LI-Medium"),"Unexpected production partition")
            from sequence_processing import _iso_date
            _iso_date(values[1],"partition date")
            _require(tuple(values) not in seen,"Duplicate Catalog partition")
            seen.add(tuple(values))
            location=part.get("StorageDescriptor",{}).get("Location","")
            _key(location)
            _require(location.startswith(ROOT+"dataset="+values[0]+"/") and location.endswith("/"),"Partition escapes its dataset production root")
            expected=_storage_view(table_input()["StorageDescriptor"])
            actual=_storage_view(part["StorageDescriptor"])
            expected["Location"]=actual["Location"]
            _require(actual==expected,"Incompatible existing partition schema")
            partitions.append(part)
        token=page.get("NextToken")
        if token is None:
            break
        _require(isinstance(token,str) and token and token not in tokens,"Invalid Catalog pagination")
        tokens.add(token)
    return table,sorted(partitions,key=lambda p:p["Values"])


def load_generation_handoff(config, s3_client):
    """Read only the fixed immutable generation manifest, never an arbitrary URI."""
    config=validate_full_config(config)
    root=STAGING_ROOT+"dataset="+config["dataset"]+"/"+config["run_id"]+"/"+config["attempt_id"]+"/"
    manifest,etag=_Store(s3_client).json(root+"metadata/run.json")
    return dict(manifest=manifest,etag=etag,manifest_uri=root+"metadata/run.json")


def _validated_generation(config, handoff, store, spark, transport, *, validate_rows=True):
    from sequence_staging import SEQUENCE_SCHEMA,validate_sequence_output
    from sequence_source import fingerprint_source_inventory
    actual=load_generation_handoff(config,store.client)
    _require(actual==handoff,"Generation manifest missing, altered or stale")
    manifest=actual["manifest"]
    days=list(owner_dates(config["dataset"]))
    schema=[dict(name=f.name,type=f.dataType.simpleString()) for f in SEQUENCE_SCHEMA]
    builder=dict(version=config["builder_version"],fingerprint=config["builder_fingerprint"])
    _require(manifest.get("manifest_kind")=="full_sequence_generation_handoff"
        and manifest.get("state")=="STAGED_VALIDATED" and manifest.get("published") is False
        and manifest.get("catalog_updated") is False and manifest.get("publication_required") is True
        and manifest.get("dataset")==config["dataset"] and manifest.get("run_id")==config["run_id"]
        and manifest.get("attempt_id")==config["attempt_id"] and manifest.get("owner_token")==config["owner_token"]
        and manifest.get("owner_dates")==days and manifest.get("source_dates")==days
        and manifest.get("source")==config["expected_source"] and manifest.get("source_after")==config["expected_source"]
        and manifest.get("schema")==schema and manifest.get("builder")==builder and manifest.get("rule_version")=="v1",
        "Only complete matching HI/LI generation handoffs can publish")
    profile=manifest.get("source_validation",{})
    spec=dataset_spec(config["dataset"])
    _require(profile.get("row_count")==spec["rows"] and profile.get("laundering_rows")==spec["laundering"]
        and profile.get("identity_check_passed") is True and profile.get("full_selected_snapshot_checked") is True
        and profile.get("verification_scope")=="provided_dataframe_only" and profile.get("dataset_wide_verified") is False
        and profile.get("checked_dates")==days and profile.get("source")==config["expected_source"],
        "Missing full selected-source validation/identity evidence")
    batches=manifest.get("batches",[])
    _require([b.get("start_date") for b in batches]==days,"Incomplete/duplicate owner-date coverage")
    staged=[]
    for batch in batches:
        day=batch["start_date"]
        batch_root=actual["manifest_uri"].removesuffix("metadata/run.json")+"batches/"+day+"/"
        _require(batch.get("receipt_uri")==batch_root+"metadata/validation.json"
            and batch.get("staging_location")==batch_root+"data/","Invalid staged receipt location")
        receipt,_=store.json(batch["receipt_uri"])
        _require(_digest(receipt)==batch.get("receipt_fingerprint") and all(batch.get(k)==v for k,v in receipt.items()),"Altered selected staging receipt")
        status,_=store.json(batch_root+"metadata/status.json")
        counts=_counts(receipt.get("counts"))
        _require(status.get("state")=="VALIDATED" and status.get("revision")==2
            and status.get("run_id")==config["run_id"] and status.get("attempt_id")==config["attempt_id"]
            and status.get("owner_token")==config["owner_token"] and status.get("dataset")==config["dataset"]
            and status.get("start_date")==day and status.get("timezone")=="UTC"
            and _counts(status.get("expected_counts"))==counts
            and receipt.get("validation_status")=="VALIDATED" and receipt.get("schema_valid") is True
            and receipt.get("values_valid") is True and receipt.get("row_count")==sum(counts.values())
            and receipt.get("source")==config["expected_source"] and receipt.get("schema")==schema
            and receipt.get("builder")==builder and receipt.get("rule_version")=="v1"
            and receipt.get("run_id")==config["run_id"] and receipt.get("attempt_id")==config["attempt_id"]
            and receipt.get("dataset")==config["dataset"] and receipt.get("start_date")==day,
            "Incomplete staging validation/status evidence")
        records=store.inventory(batch_root+"data/")
        _require(records==receipt.get("files") and fingerprint_source_inventory(records)==receipt.get("inventory_fingerprint"),"Staging inventory changed")
        prefix=batch_root+"data/dataset="+config["dataset"]+"/start_date="+day+"/"
        files=[]
        for r in records:
            if r["path"]==batch_root+"data/_SUCCESS":
                continue
            _require(r["path"].startswith(prefix) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*[.]parquet",r["path"][len(prefix):])
                and r["size"]>0,"Unexpected staged data object")
            files.append(r)
        _require(len(files)==receipt.get("parquet_files") and bool(files)==bool(sum(counts.values()))
            and sum(r["size"] for r in records)==receipt.get("size_bytes"),"Staged inventory/count mismatch")
        if validate_rows and files:
            frame=transport.read(spark,batch_root+"data/",[r["path"] for r in files],status)
            validate_sequence_output(frame,config["dataset"],day,"UTC",counts)
        _require(store.inventory(batch_root+"data/")==records,"Staging changed during validation")
        staged.append(dict(start_date=day,counts={str(k):v for k,v in counts.items()},files=files,
                           size_bytes=receipt["size_bytes"]))
    totals={str(k):sum(b["counts"][str(k)] for b in staged) for k in (2,3)}
    _require(manifest.get("counts")==totals and manifest.get("row_count")==sum(totals.values())
        and manifest.get("parquet_files")==sum(len(b["files"]) for b in staged)
        and manifest.get("size_bytes")==sum(b["size_bytes"] for b in staged),"Run totals disagree with validated batches")
    _require(load_generation_handoff(config,store.client)==actual,"Generation handoff changed during validation")
    return manifest,staged


def publish_full_generation(config, handoff, *, allow_publication=False, readers_paused=False,
                             maintenance_evidence, max_copy_bytes, resume=False,
                             glue_context, catalog_client, s3_client, identity_client, transport=None):
    """Publish one entire dataset, or explicitly resume the SAME owned attempt.

    Success requires independent staged/destination Spark validation, current
    inventories, exact Catalog read-back, durable receipt and lock release.
    Failure retains lock/journal/files; readers must stay paused. Explicit resume
    classifies every partition as OLD/NEW/CONFLICT. No automatic rollback/deletion.
    """
    _require(allow_publication is True and readers_paused is True,"Explicit publication activation and reader pause required")
    _require(type(resume) is bool and type(max_copy_bytes) is int and max_copy_bytes>0
        and isinstance(maintenance_evidence,str) and maintenance_evidence.strip(),"Explicit maintenance evidence/copy bound required")
    config=validate_full_config(config)
    from sequence_glue_runtime import validate_s3_conditional_writes
    from sequence_staging import validate_sequence_output
    spark=glue_context.spark_session
    _require(spark.conf.get("spark.sql.session.timeZone")=="UTC","Matching UTC session required")
    for client in (catalog_client,s3_client,identity_client):
        _require(client.meta.region_name=="us-east-1","Wrong AWS client region")
    _require(identity_client.get_caller_identity().get("Account")==config["account_id"],"Wrong AWS account")
    validate_s3_conditional_writes(s3_client)
    _require("ChecksumSHA256" in s3_client.meta.service_model.operation_model("PutObject").input_shape.members,"SDK checksum support required")
    store,transport=_Store(s3_client),transport or _SparkParquetTransport()
    started=time.monotonic()
    def deadline():
        _require(time.monotonic()-started < config["max_run_seconds"],"Publication time budget exhausted")
    def source_check():
        deadline()
        observed=inspect_full_source(catalog_client,s3_client,config["dataset"],verified_empty_dates=config["verified_empty_dates"])
        _require(snapshot_identity(observed)==config["expected_source"],"Frozen source/Catalog changed")
    source_check()
    manifest,staged=_validated_generation(config,handoff,store,spark,transport)
    identity=dict(dataset=config["dataset"],run_id=config["run_id"],attempt_id=config["attempt_id"],owner_token=config["owner_token"])
    version=ROOT+"dataset="+config["dataset"]+"/versions/"+config["run_id"]+"/"+config["attempt_id"]+"/"
    uri=METADATA+"dataset="+config["dataset"]+"/"+config["run_id"]+"/"+config["attempt_id"]+"/publication.json"
    binding=dict(identity=identity,generation_manifest_uri=handoff["manifest_uri"],generation_fingerprint=_digest(manifest),
        source=config["expected_source"],version_root=version,counts=manifest["counts"],row_count=manifest["row_count"])
    for b in staged:
        _require(all(r["size"]<=max_copy_bytes for r in b["files"]),"Parquet file exceeds explicit in-memory copy bound")
    saved=store.json(uri,optional=True)
    lock=store.json(LOCK,optional=True)
    if saved is None:
        _require(lock is None or lock[0].get("state")=="RELEASED","Another publisher owns the table; no takeover")
        _require(not store.inventory(version),"Existing production version cannot be reused")
        table,parts=_catalog(catalog_client)
        _require(not any(p["StorageDescriptor"]["Location"].startswith(version) for p in parts),
            "Catalog references a version with missing publication evidence")
        chosen=[p for p in parts if p["Values"][0]==config["dataset"]]
        others=[_partition_view(p) for p in parts if p["Values"][0]!=config["dataset"]]
        old={p["Values"][1]:p for p in chosen}
        actions=[]
        for day in sorted(set(owner_dates(config["dataset"]))|set(old)):
            after=None
            if day in owner_dates(config["dataset"]):
                after=dict(Values=[config["dataset"],day],StorageDescriptor=dict(table_input()["StorageDescriptor"],
                    Location=version+"data/dataset="+config["dataset"]+"/start_date="+day+"/"),
                    Parameters={"role5_publication_receipt":uri,"role5_rule_version":"v1"})
            previous=old.get(day)
            actions.append(dict(start_date=day,before=_serial(previous),after=after,state="PENDING",
                previous_inventory=store.inventory(previous["StorageDescriptor"]["Location"]) if previous else []))
        receipt=dict(format_version=1,manifest_kind="full_sequence_publication",state="PREPARING",revision=0,
            binding=binding,previous_table=_serial(table),other_partitions=others,actions=actions,copied=[],
            maintenance_evidence=maintenance_evidence,created_at=_now(),published=False)
        receipt["plan_fingerprint"]=_plan_fingerprint(receipt)
        lock_value=dict(state="LOCKED",owner=identity,publication_uri=uri,revision=(lock[0].get("revision",0)+1 if lock else 0))
        lock_etag=store.put(LOCK,lock_value,lock[1] if lock else None)
        # If exclusive receipt creation is uncertain, retain ownership for review.
        etag=store.put(uri,receipt)
    else:
        _require(resume,"Publication attempt already exists; explicit recovery required")
        receipt,etag=saved
        _require(receipt.get("binding")==binding and receipt.get("manifest_kind")=="full_sequence_publication","Stale/mismatched publication recovery")
        _require(receipt.get("state") in ("PREPARING","PREPARED","PUBLISHING","RECOVERY_REQUIRED","PUBLISHED")
            and type(receipt.get("revision")) is int and receipt["revision"]>=0
            and receipt.get("plan_fingerprint")==_plan_fingerprint(receipt),"Invalid publication recovery state/plan")
        _require(lock is not None and lock[0].get("owner")==identity and lock[0].get("publication_uri")==uri,
            "Recovery requires original table-lock owner")
        lock_value,lock_etag=lock
        _require(lock_value["state"]=="LOCKED" or (lock_value["state"]=="RELEASED" and receipt["state"]=="PUBLISHED"),"Invalid recovery ownership")

    # Treat persisted recovery plans as a trust boundary too: no saved action
    # may redirect this dataset's Catalog writes to another dataset or location.
    dates=list(owner_dates(config["dataset"]))
    actions=receipt["actions"]
    action_days=[a["start_date"] for a in actions]
    _require(action_days==sorted(set(action_days)) and set(dates)<=set(action_days),"Invalid recovery owner-date coverage")
    for action in actions:
        day=action["start_date"]
        from sequence_processing import _iso_date
        _iso_date(day,"publication action date")
        before,after=action["before"],action["after"]
        _require(action.get("state") in ("PENDING","INTENT_RECORDED","VERIFIED"),"Invalid operation journal state")
        if day in dates:
            expected=dict(Values=[config["dataset"],day],StorageDescriptor=dict(table_input()["StorageDescriptor"],
                Location=version+"data/dataset="+config["dataset"]+"/start_date="+day+"/"),
                Parameters={"role5_publication_receipt":uri,"role5_rule_version":"v1"})
            _require(after==expected,"Recovery action escapes the selected dataset/version")
        else:
            _require(after is None and before is not None,"Unexpected recovery stale-date action")
        if before is not None:
            _require(before.get("Values")==[config["dataset"],day]
                and before.get("StorageDescriptor",{}).get("Location","").startswith(ROOT+"dataset="+config["dataset"]+"/"),
                "Recovery old definition escapes selected dataset")
            _key(before["StorageDescriptor"]["Location"])

    def owned():
        observed=store.json(LOCK)
        _require(observed==(lock_value,lock_etag),"Publication lock changed")
        current=store.json(uri)
        _require(current==(receipt,etag),"Publication receipt revision changed")

    def save(**changes):
        nonlocal receipt,etag
        owned()
        pending=dict(receipt,**changes,revision=receipt["revision"]+1,updated_at=_now())
        etag=store.put(uri,pending,etag)
        receipt=pending

    if receipt["state"]=="PUBLISHED":
        # Terminal receipts are immutable. Recovery here can only reverify the
        # same publication and release its still-owned lock after a lost reply.
        owned()
        _require(receipt.get("verified") is True and receipt.get("published") is True,"Invalid terminal receipt")
        _require(store.inventory(version+"data/")==receipt["destination_inventory"],"Published files changed")
        for file in receipt["copied"]:
            _require(hashlib.sha256(store.get(file["destination"])[0]).hexdigest()==file["sha256"],"Published checksum changed")
        for batch in staged:
            paths=[c["destination"] for c in receipt["copied"] if "/start_date="+batch["start_date"]+"/" in c["destination"]]
            if paths:
                frame=transport.read(spark,version+"data/",paths,dict(timezone="UTC"))
                validate_sequence_output(frame,config["dataset"],batch["start_date"],"UTC",_counts(batch["counts"]))
        table,parts=_catalog(catalog_client)
        _require([_partition_view(p) for p in parts if p["Values"][0]==config["dataset"]]
            ==[_partition_view(a["after"]) for a in receipt["actions"] if a["after"] is not None]
            and [_partition_view(p) for p in parts if p["Values"][0]!=config["dataset"]]==receipt["other_partitions"],"Published Catalog state changed")
        _require(_digest(_serial(dict(table=table,partitions=parts)))==receipt["catalog_fingerprint"],"Published Catalog fingerprint changed")
        source_check()
        if lock_value["state"]=="LOCKED":
            released=dict(lock_value,state="RELEASED",revision=lock_value["revision"]+1)
            store.put(LOCK,released,lock_etag)
        return dict(publication_uri=uri,publication=deepcopy(receipt),published=True,catalog_updated=True,
            table=DATABASE+"."+TABLE,counts=manifest["counts"],row_count=manifest["row_count"])

    try:
        owned()
        # Copy/read-back every allowlisted file. Recovery may reuse ONLY byte-
        # identical files belonging to this durably owned version attempt.
        expected_paths=set()
        copied=[]
        for batch in staged:
            deadline()
            for file in batch["files"]:
                destination=version+"data/dataset="+config["dataset"]+"/start_date="+batch["start_date"]+"/"+file["path"].rsplit("/",1)[1]
                expected_paths.add(destination)
                raw,_=store.get(file["path"],IfMatch=file["etag"])
                _require(len(raw)==file["size"],"Staged file size changed")
                digest=hashlib.sha256(raw).hexdigest()
                existing=store.get(destination,optional=True)
                if existing is not None:
                    _require(resume and hashlib.sha256(existing[0]).hexdigest()==digest,"Existing destination conflict")
                else:
                    owned()
                    s3_client.put_object(Bucket=BUCKET,Key=_key(destination),Body=raw,IfNoneMatch="*",
                        ServerSideEncryption="AES256",ChecksumSHA256=base64.b64encode(hashlib.sha256(raw).digest()).decode())
                _require(hashlib.sha256(store.get(destination)[0]).hexdigest()==digest,"Destination SHA-256 mismatch")
                copied.append(dict(source=file["path"],destination=destination,size_bytes=len(raw),sha256=digest))
                save(copied=deepcopy(copied))
        files=store.inventory(version+"data/")
        _require({r["path"] for r in files}==expected_paths,"Unexpected/missing production files")
        for batch in staged:
            paths=[c["destination"] for c in copied if "/start_date="+batch["start_date"]+"/" in c["destination"]]
            if paths:
                frame=transport.read(spark,version+"data/",paths,dict(timezone="UTC"))
                validate_sequence_output(frame,config["dataset"],batch["start_date"],"UTC",_counts(batch["counts"]))
        _require(store.inventory(version+"data/")==files,"Production files changed during independent validation")
        _validated_generation(config,handoff,store,spark,transport,validate_rows=False)
        source_check()
        save(state="PREPARED",destination_inventory=files)
        table,parts=_catalog(catalog_client)
        _require([_partition_view(p) for p in parts if p["Values"][0]!=config["dataset"]]==receipt["other_partitions"],"Other dataset Catalog state changed")
        previous=receipt["previous_table"]
        if table is None:
            _require(previous is None,"Production table disappeared")
            save(state="PUBLISHING",table_action="CREATE_INTENT_RECORDED")
            owned()
            catalog_client.create_table(DatabaseName=DATABASE,TableInput=table_input())
            table,parts=_catalog(catalog_client)
            _require(table is not None,"Table creation unverified")
        else:
            if previous is None:
                _require(resume and receipt.get("table_action")=="CREATE_INTENT_RECORDED",
                    "Unexpected table creation after planning")
            else:
                _require(_digest(_serial(table))==_digest(previous),"Stale production table definition")
        # After an interrupted table creation, compatibility above is checked;
        # an independently created identical table still requires same lock owner.
        table_fingerprint=_digest(_serial(table))
        for index,action in enumerate(receipt["actions"]):
            deadline();owned()
            current_table,current_parts=_catalog(catalog_client)
            _require(_digest(_serial(current_table))==table_fingerprint,"Table changed during publication")
            _require([_partition_view(p) for p in current_parts if p["Values"][0]!=config["dataset"]]==receipt["other_partitions"],"Other dataset changed")
            observed=next((p for p in current_parts if p["Values"]==[config["dataset"],action["start_date"]]),None)
            before,after=action["before"],action["after"]
            if _partition_view(observed)==_partition_view(after):
                classification="INTENDED_NEW"
            elif _partition_view(observed)==_partition_view(before):
                classification="EXPECTED_OLD"
            else:
                raise ValueError("Conflicting Catalog partition; recovery requires review")
            if classification=="EXPECTED_OLD":
                if before:
                    _require(store.inventory(before["StorageDescriptor"]["Location"])==action["previous_inventory"],"Previous published files changed")
                actions=deepcopy(receipt["actions"]);actions[index]["state"]="INTENT_RECORDED"
                save(state="PUBLISHING",actions=actions)
                owned()
                if after is None:
                    catalog_client.delete_partition(DatabaseName=DATABASE,TableName=TABLE,PartitionValues=before["Values"])
                elif before is None:
                    catalog_client.create_partition(DatabaseName=DATABASE,TableName=TABLE,PartitionInput=after)
                else:
                    catalog_client.update_partition(DatabaseName=DATABASE,TableName=TABLE,
                        PartitionValueList=before["Values"],PartitionInput=after)
            _,current_parts=_catalog(catalog_client)
            observed=next((p for p in current_parts if p["Values"]==[config["dataset"],action["start_date"]]),None)
            _require(_partition_view(observed)==_partition_view(after),"Catalog action read-back failed")
            actions=deepcopy(receipt["actions"]);actions[index].update(state="VERIFIED",observed="INTENDED_NEW")
            save(actions=actions)
        table,parts=_catalog(catalog_client)
        _require(_digest(_serial(table))==table_fingerprint,"Final table definition changed")
        selected=[_partition_view(p) for p in parts if p["Values"][0]==config["dataset"]]
        intended=[_partition_view(a["after"]) for a in receipt["actions"] if a["after"] is not None]
        _require(selected==intended and [ _partition_view(p) for p in parts if p["Values"][0]!=config["dataset"]]==receipt["other_partitions"],"Final Catalog coverage/reconciliation failed")
        _require(store.inventory(version+"data/")==files,"Final production inventory mismatch")
        source_check()
        save(state="PUBLISHED",published=True,catalog_updated=True,verified=True,
            catalog_fingerprint=_digest(_serial(dict(table=table,partitions=parts))),verified_at=_now())
        if lock_value["state"]=="LOCKED":
            lock_value=dict(lock_value,state="RELEASED",revision=lock_value["revision"]+1)
            lock_etag=store.put(LOCK,lock_value,lock_etag)
        _require(store.json(uri)==(receipt,etag) and store.json(LOCK)==(lock_value,lock_etag),"Publication completion unconfirmed")
        return dict(publication_uri=uri,publication=deepcopy(receipt),published=True,catalog_updated=True,
                    table=DATABASE+"."+TABLE,counts=manifest["counts"],row_count=manifest["row_count"])
    except Exception as original:
        try:
            if receipt["state"]!="PUBLISHED":
                save(state="RECOVERY_REQUIRED",error_type=type(original).__name__,published=False)
        except Exception as recording:
            original.add_note("Publication failure recording unavailable: "+type(recording).__name__)
        raise
