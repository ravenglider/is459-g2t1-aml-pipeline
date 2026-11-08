"""Isolated batching kernel; NOT wired to a Glue job or publication.

The existing adaptive splitter processes a UNION of disjoint A roots once.
The root_chunks=1 view is internal scheduling only: original hash ownership is
recomputed and reconciled for every output. Consumers run inside owned caches.
No staging success or durable root completion is claimed by this kernel.
"""
from dataclasses import replace
import hashlib
from pyspark.sql import functions as F
from sequence_chunked_generation import ChunkLimits,generate_date_chunks
from sequence_batch import _enforce_limit
from sequence_staging import validate_sequence_output


def generate_root_group(source,dataset,plan,limits,roots,*,budget,consume,
    max_group_input_rows,max_group_pair_work,max_group_output_rows):
    """Emit bounded, analytically validated 21-column chunks to an injected sink.

    Sink gets (output, counts, root_counts, ownership) while output is cached.
    A deployable sink must atomically commit the WHOLE group's root proofs after
    immutable Parquet write/read-back; this function does not replace that layer.
    """
    if not isinstance(limits,ChunkLimits) or budget.limits!=limits:raise ValueError("Matching explicit limits required")
    if any(type(v) is not int or v<=0 for v in (max_group_input_rows,max_group_pair_work,max_group_output_rows)):
        raise ValueError("Explicit positive group spending limits required")
    roots=list(roots)
    if (not roots or len(roots)!=len(set(roots)) or any(type(r) is not int or not 0<=r<limits.root_chunks for r in roots)):
        raise ValueError("Explicit disjoint root group required")
    if not callable(consume):raise ValueError("Explicit consuming sink required")
    roots=sorted(roots);day=plan.start_date.isoformat()
    owners=source.where(F.col("txn_date")==day).withColumn("_owner_root",
        F.pmod(F.xxhash64("dataset","txn_id"),F.lit(limits.root_chunks)))
    selected=owners.where(F.col("_owner_root").isin(roots))
    budget.deadline()
    input_counts={r:0 for r in roots}
    input_counts.update({int(r._owner_root):r["count"] for r in selected.groupBy("_owner_root").count().collect()})
    budget.deadline()
    _enforce_limit("group_input_rows",sum(input_counts.values()),max_group_input_rows)
    totals={r:{2:0,3:0} for r in roots};chunks=[]
    label=hashlib.sha256((day+":"+",".join(map(str,roots))).encode()).hexdigest()[:20]
    virtual=replace(limits,root_chunks=1)

    class BudgetView:
        limits=virtual
        pair_work=0
        output_rows=0
        def deadline(self):budget.deadline()
        def node(self):budget.node()
        def pairs(self,n):
            # The bounded pair join has already run: retain its actual work in
            # the date budget even when this group must stop.
            budget.pairs(n);self.pair_work+=n
            _enforce_limit("group_pair_work",self.pair_work,max_group_pair_work)
        def expansion(self,n):
            _enforce_limit("group_output_rows",self.output_rows+n,max_group_output_rows)
            budget.expansion(n);self.output_rows+=n

    class Sink:
        _batched_kernel=True
        def begin_date(self,owner_day,root_count,rows):
            if owner_day!=day or root_count!=1 or rows!=sum(input_counts.values()):raise ValueError("Group input coverage mismatch")
        def write_chunk(self,owner_day,root,cid,output,counts,*,ownership):
            if owner_day!=day or root!=0:raise ValueError("Invalid internal group ownership")
            validate_sequence_output(output,dataset,day,"UTC",counts)
            grouped=output.withColumn("_owner_root",F.pmod(F.xxhash64("dataset",F.element_at("txn_ids",1)),F.lit(limits.root_chunks)))
            observed={r:{2:0,3:0} for r in roots}
            for row in grouped.groupBy("_owner_root","sequence_length").count().collect():
                if row._owner_root not in observed:raise ValueError("Output escapes requested first-transaction roots")
                observed[row._owner_root][row.sequence_length]=row["count"]
            if {k:sum(v[k] for v in observed.values()) for k in (2,3)}!=counts:raise ValueError("Group counts disagree")
            path="group"+label+":"+ownership
            consume(output,counts,observed,path)
            for r in roots:
                for k in (2,3):totals[r][k]+=observed[r][k]
            chunk_id="c"+hashlib.sha256(path.encode()).hexdigest()[:60]
            chunks.append(chunk_id)
            return dict(chunk_id=chunk_id)
        def complete_root(self,owner_day,root,rows,counts,cids):
            if rows!=sum(input_counts.values()) or cids!=chunks or counts!={k:sum(v[k] for v in totals.values()) for k in (2,3)}:
                raise ValueError("Incomplete group coverage")
            return dict(state="GENERATED_NOT_STAGED")
        def finalize_date(self,*a,**k):raise AssertionError("Kernel cannot finalize a date")

    group_budget=BudgetView()
    generate_date_chunks(source,dataset,plan,virtual,Sink(),budget=group_budget,selected_root=0,
        _owner_ids=selected.select("dataset","txn_id"))
    return dict(state="GENERATED_NOT_STAGED",dataset=dataset,start_date=day,roots=roots,
        input_rows=input_counts,counts=totals,chunks=chunks,published=False,catalog_updated=False,
        materialized_pair_work=group_budget.pair_work,reserved_output_rows=group_budget.output_rows)
