"""Immutable chunk-work attempts, then one compatible validated date receipt.

No production or Catalog API. Interrupted work is retained and NEVER resumed or
overwritten. A fresh run/attempt is required. Final Parquet names are flattened
under the existing date contract; no append or giant in-memory union is used.
"""
import base64
from copy import deepcopy
import hashlib
from sequence_full_staging import FullS3Stager
from sequence_full_source import BUCKET,inventory_objects
from sequence_manifest import _digest,_token
from sequence_s3 import _put,_get
from sequence_source import fingerprint_source_inventory
from sequence_staging import validate_sequence_output


class ChunkS3Stager(FullS3Stager):
    def __init__(self,client,config,*,spark,max_copy_bytes,transport=None,deadline=lambda:None):
        super().__init__(client,config,transport=transport)
        if type(max_copy_bytes) is not int or max_copy_bytes<=0:raise ValueError("Explicit copy-byte bound required")
        self.max_copy_bytes=max_copy_bytes;self.deadline=deadline;self.dates={};self.spark=spark

    def _owned_date(self,day):
        self.deadline();self._assert_owner()
        date=self.dates[day]
        actual,etag,_=_get(self.client,date["status_uri"])
        if (actual,etag)!=(date["status"],date["etag"]):raise ValueError("Date ownership changed")
        return date

    def begin_date(self,day,root_chunks,owner_rows):
        self._assert_owner()
        if day in self.dates or day not in self.manifest["owner_dates"]:raise ValueError("Existing/unowned date")
        if type(root_chunks) is not int or root_chunks<=0 or type(owner_rows) is not int or owner_rows<0:
            raise ValueError("Explicit complete root ownership required")
        root=self.root+"batches/"+day+"/";work=self.root+"chunk-work/"+day+"/"
        if inventory_objects(self.client,root) or inventory_objects(self.client,work):raise ValueError("Date attempt already exists")
        status=dict(format_version=1,state="WRITING",revision=0,dataset=self.config["dataset"],start_date=day,
            timezone="UTC",owner_token=self.config["owner_token"],run_id=self.config["run_id"],attempt_id=self.config["attempt_id"],
            root_chunks=root_chunks,owner_rows=owner_rows)
        uri=root+"metadata/status.json";etag=_put(self.client,uri,status)
        self.dates[day]=dict(root=root,work=work,status_uri=uri,status=status,etag=etag,chunks={},roots={})
        self._owned_date(day)

    def write_chunk(self,day,root,chunk_id,output,counts,*,ownership):
        date=self._owned_date(day);_token(chunk_id)
        if date["status"]["state"]!="WRITING":raise ValueError("Date no longer writable")
        if type(root) is not int or not 0<=root<date["status"]["root_chunks"] or root in date["roots"]:
            raise ValueError("Root not writable")
        if chunk_id in date["chunks"]:raise ValueError("Duplicate chunk")
        location=date["work"]+chunk_id+"/"
        if inventory_objects(self.client,location):raise ValueError("Chunk attempt exists")
        status=dict(state="WRITING",revision=0,timezone="UTC",root=root,chunk_id=chunk_id,ownership=ownership,
            owner_token=self.config["owner_token"],expected_counts={str(k):v for k,v in counts.items()})
        status_uri=location+"status.json";etag=_put(self.client,status_uri,status)
        validation=validate_sequence_output(output,self.config["dataset"],day,"UTC",counts)
        if validation["row_count"]:self.transport.write(output,location+"data/",self.config["max_records_per_file"])
        records=inventory_objects(self.client,location+"data/")
        prefix=location+"data/dataset="+self.config["dataset"]+"/start_date="+day+"/"
        files=[]
        for record in records:
            path=record["path"]
            if path==location+"data/_SUCCESS":continue
            if not path.startswith(prefix) or "/" in path[len(prefix):] or not path.endswith(".parquet") or record["size"]<=0:
                raise ValueError("Unexpected chunk artifact")
            if record["size"]>self.max_copy_bytes:raise ValueError("Chunk file exceeds explicit copy-byte bound")
            files.append(path)
        if bool(files)!=bool(validation["row_count"]):raise ValueError("Empty chunk mismatch")
        back=self.transport.read(output.sparkSession,location+"data/",files,status)
        validate_sequence_output(back,self.config["dataset"],day,"UTC",counts)
        if inventory_objects(self.client,location+"data/")!=records:raise ValueError("Chunk changed during validation")
        self._owned_date(day)
        observed,token,_=_get(self.client,status_uri)
        if (observed,token)!=(status,etag):raise ValueError("Chunk ownership changed")
        receipt=dict(chunk_id=chunk_id,root=root,ownership=ownership,counts={str(k):v for k,v in counts.items()},
            files=records,inventory_fingerprint=fingerprint_source_inventory(records),status="VALIDATED")
        _put(self.client,location+"validation.json",receipt)
        _put(self.client,status_uri,dict(status,state="VALIDATED",revision=1),etag)
        date["chunks"][chunk_id]=dict(receipt,receipt_uri=location+"validation.json")
        return deepcopy(receipt)

    def complete_root(self,day,root,input_rows,counts,chunk_ids):
        date=self._owned_date(day)
        if date["status"]["state"]!="WRITING":raise ValueError("Date no longer writable")
        if root in date["roots"] or type(root) is not int or not 0<=root<date["status"]["root_chunks"]:
            raise ValueError("Root already complete/unowned")
        actual={cid for cid,c in date["chunks"].items() if c["root"]==root}
        if len(chunk_ids)!=len(set(chunk_ids)) or set(chunk_ids)!=actual:raise ValueError("Incomplete chunk coverage")
        totals={str(k):sum(date["chunks"][cid]["counts"][str(k)] for cid in chunk_ids) for k in (2,3)}
        if totals!={str(k):v for k,v in counts.items()} or type(input_rows) is not int or input_rows<0:
            raise ValueError("Root counts disagree")
        receipt=dict(root=root,input_rows=input_rows,counts=totals,chunk_ids=chunk_ids,state="COMPLETE")
        _put(self.client,date["work"]+"root-"+str(root)+".json",receipt)
        date["roots"][root]=receipt
        return deepcopy(receipt)

    def finalize_date(self,day,expected_counts):
        date=self._owned_date(day)
        if date["status"]["state"]!="WRITING":raise ValueError("Date already finalizing/finalized")
        if set(date["roots"])!=set(range(date["status"]["root_chunks"])):
            raise ValueError("Partial roots cannot finalize a date")
        for root,expected in date["roots"].items():
            actual,_,_=_get(self.client,date["work"]+"root-"+str(root)+".json")
            if actual!=expected:raise ValueError("Root completion evidence changed")
        if sum(r["input_rows"] for r in date["roots"].values())!=date["status"]["owner_rows"]:
            raise ValueError("First-transaction ownership coverage mismatch")
        counts={str(k):sum(r["counts"][str(k)] for r in date["roots"].values()) for k in (2,3)}
        if counts!={str(k):v for k,v in expected_counts.items()}:raise ValueError("Date projection mismatch")
        allowed={date["work"]+"root-"+str(r)+".json" for r in date["roots"]}
        for chunk in date["chunks"].values():
            allowed.update(r["path"] for r in chunk["files"])
            allowed.update((chunk["receipt_uri"],chunk["receipt_uri"].removesuffix("validation.json")+"status.json"))
        if {r["path"] for r in inventory_objects(self.client,date["work"])}!=allowed:
            raise ValueError("Unexpected/incomplete chunk-work artifacts")
        paths=[]
        for cid,chunk in sorted(date["chunks"].items()):
            self._owned_date(day)
            receipt,_,_=_get(self.client,chunk["receipt_uri"])
            if receipt!={k:v for k,v in chunk.items() if k!="receipt_uri"}:raise ValueError("Chunk receipt changed")
            location=chunk["receipt_uri"].removesuffix("validation.json")
            status,_,_=_get(self.client,location+"status.json")
            if status.get("state")!="VALIDATED" or status.get("owner_token")!=self.config["owner_token"]:
                raise ValueError("Chunk is not validated/owned")
            if inventory_objects(self.client,location+"data/")!=chunk["files"]:raise ValueError("Chunk inventory changed")
            for index,record in enumerate(chunk["files"]):
                if not record["path"].endswith(".parquet"):continue
                if record["size"]>self.max_copy_bytes:raise ValueError("Chunk file exceeds explicit copy-byte bound")
                self._owned_date(day)
                response=self.client.get_object(Bucket=BUCKET,Key=record["path"].split("/",3)[3],IfMatch=record["etag"])
                try:raw=response["Body"].read()
                finally:response["Body"].close()
                if len(raw)!=record["size"]:raise ValueError("Source chunk size changed")
                uri=date["root"]+"data/dataset="+self.config["dataset"]+"/start_date="+day+"/"+cid+"-"+str(index)+".parquet"
                self.client.put_object(Bucket=BUCKET,Key=uri.split("/",3)[3],Body=raw,IfNoneMatch="*",
                    ServerSideEncryption="AES256",ChecksumSHA256=base64.b64encode(hashlib.sha256(raw).digest()).decode())
                check=self.client.get_object(Bucket=BUCKET,Key=uri.split("/",3)[3])
                try:copied=check["Body"].read()
                finally:check["Body"].close()
                if hashlib.sha256(copied).digest()!=hashlib.sha256(raw).digest():raise ValueError("Final date copy checksum mismatch")
                paths.append(uri)
        self._owned_date(day)
        status=dict(date["status"],state="VALIDATING",revision=1,expected_counts=counts)
        date["etag"]=_put(self.client,date["status_uri"],status,date["etag"]);date["status"]=status
        records=inventory_objects(self.client,date["root"]+"data/")
        if {r["path"] for r in records}!=set(paths):raise ValueError("Unexpected final date files")
        back=self.transport.read(self.spark,date["root"]+"data/",paths,status)
        report=validate_sequence_output(back,self.config["dataset"],day,"UTC",{int(k):v for k,v in counts.items()})
        if inventory_objects(self.client,date["root"]+"data/")!=records:raise ValueError("Final date inventory changed")
        receipt=dict(dataset=self.config["dataset"],start_date=day,run_id=self.config["run_id"],attempt_id=self.config["attempt_id"],
            staging_location=date["root"]+"data/",files=records,inventory_fingerprint=fingerprint_source_inventory(records),
            parquet_files=len(paths),size_bytes=sum(r["size"] for r in records),**report,validation_status="VALIDATED",
            source=self.manifest["source"],schema=self.manifest["schema"],builder=self.manifest["builder"],rule_version="v1")
        receipt["counts"]=counts
        uri=date["root"]+"metadata/validation.json";_put(self.client,uri,receipt)
        self._owned_date(day)
        status=dict(status,state="VALIDATED",revision=2)
        date["etag"]=_put(self.client,date["status_uri"],status,date["etag"]);date["status"]=status
        actual,_,_=_get(self.client,uri)
        if actual!=receipt:raise ValueError("Unconfirmed date receipt")
        return dict(receipt,receipt_uri=uri,receipt_fingerprint=_digest(receipt))
