"""Real Spark batch-kernel equivalence; no AWS boundaries are invoked."""
import json,os,sys,time,unittest
from pathlib import Path
from unittest.mock import patch,Mock
from functools import partial
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]
from fixtures import transaction
from sequence_source import CLEANED_SCHEMA
from sequence_processing import plan_start_date_batches
from sequence_chunked_generation import ChunkLimits,ChunkBudget
import sequence_chunked_generation as core
from sequence_batched_kernel import generate_root_group as generate
from sequence_batch import GrowthLimitExceeded
generate_root_group=partial(generate,max_group_input_rows=10000,max_group_pair_work=10000,max_group_output_rows=10000)
from bq2_sequence_builder import build_two_hop_links,build_three_hop_links,build_sequence_output


class BatchKernelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        old=os.environ.get("PYSPARK_PYTHON");oldtz=os.environ.get("TZ")
        os.environ["PYSPARK_PYTHON"]=sys.executable;os.environ["TZ"]="UTC"
        if hasattr(time,"tzset"):time.tzset()
        def restore():
            if old is None:os.environ.pop("PYSPARK_PYTHON",None)
            else:os.environ["PYSPARK_PYTHON"]=old
            if oldtz is None:os.environ.pop("TZ",None)
            else:os.environ["TZ"]=oldtz
            if hasattr(time,"tzset"):time.tzset()
        cls.addClassCleanup(restore)
        cls.spark=(SparkSession.builder.master("local[2]").appName("isolated-batched-kernel")
            .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","2")
            .config("spark.ui.enabled","false").getOrCreate())
        cls.addClassCleanup(cls.spark.stop);cls.spark.sparkContext.setLogLevel("ERROR")
        rows=[transaction(i,"2022-09-01 00:00",(1,"A"+str(i)),(2,"B"),dataset="HI-Medium",label=int(i==-9))
            for i in (-9,9007199254740993,11,12,13,14)]
        rows += [transaction(i,"2022-09-02 00:00",(2,"B"),(2,"C"),dataset="HI-Medium") for i in (3,4)]
        rows += [transaction(i,"2022-09-03 00:00",(2,"C"),(3,"D"+str(i)),dataset="HI-Medium") for i in (5,6)]
        cls.frame=cls.spark.createDataFrame(rows,CLEANED_SCHEMA)
        cls.plan,=plan_start_date_batches("2022-09-01","2022-09-01",dataset_start_date="2022-09-01",dataset_end_date="2022-09-28",
            available_partition_dates=["2022-09-01","2022-09-02","2022-09-03"])
    def lim(self,**changes):
        cfg=dict(root_chunks=4,max_preceding_rows=100,max_pair_candidates=1000,max_pair_links=1000,max_projected_triples=1000,
            max_middle_expansion=1000,max_output_rows=1000,max_total_pair_links=10000,max_total_output_rows=10000,max_chunks=1000,max_split_nodes=10000)
        cfg.update(changes);return ChunkLimits(**cfg)
    def canonical(self,rows):return sorted(json.dumps(r.asDict(),sort_keys=True,default=str) for r in rows)
    def reference(self,frame=None):
        from pyspark.sql import functions as F
        frame=self.frame if frame is None else frame
        pairs=build_two_hop_links(frame)
        ids=frame.where(F.col("txn_date")=="2022-09-01").select("dataset",F.col("txn_id").alias("first_txn_id"))
        starts=pairs.join(ids,["dataset","first_txn_id"],"left_semi")
        return build_sequence_output(frame,starts,build_three_hop_links(pairs,starting_links=starts))
    def clean(self):self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(),0)
    def test_all_columns_and_one_shared_ab_bc_workflow(self):
        rows=[];limits=self.lim()
        with patch.object(core,"_build_two_hop_links_validated",wraps=core._build_two_hop_links_validated) as joins:
            result=generate_root_group(self.frame,"HI-Medium",self.plan,limits,range(4),budget=ChunkBudget(limits,lambda:None),
                consume=lambda frame,*rest:rows.extend(frame.collect()))
            self.assertEqual(joins.call_count,2)  # One AB and one shared reachable BC.
            self.assertTrue(all(call.kwargs["second_txn_ids"] is None for call in joins.call_args_list))
        self.assertEqual(self.canonical(rows),self.canonical(self.reference().collect()))
        self.assertEqual(len(rows),36);self.assertEqual(len(rows[0]),21)
        self.assertEqual(len({r.sequence_id for r in rows}),36)
        self.assertGreater(sum(n>0 for n in result["input_rows"].values()),1)
        self.assertEqual(sum(v[2] for v in result["counts"].values()),12)
        self.assertEqual(sum(v[3] for v in result["counts"].values()),24)
        self.assertEqual(result["state"],"GENERATED_NOT_STAGED");self.clean()
        from pyspark.sql import functions as F
        first=self.frame.where(F.col("txn_date")=="2022-09-01").select("dataset","txn_id")
        old=core._build_two_hop_links_validated(self.frame,first_txn_ids=first,
            second_txn_ids=self.frame.select("dataset","txn_id"),_hourly=True)
        new=core._build_two_hop_links_validated(self.frame,first_txn_ids=first,_hourly=True)
        self.assertGreater(old._jdf.queryExecution().optimizedPlan().toString().count("LeftSemi"),
            new._jdf.queryExecution().optimizedPlan().toString().count("LeftSemi"))
    def test_disjoint_groups_union_without_duplicate_sequences(self):
        rows=[];limits=self.lim(root_chunks=2);budget=ChunkBudget(limits,lambda:None);chunks=[]
        for roots in ([0],[1]):
            result=generate_root_group(self.frame,"HI-Medium",self.plan,limits,roots,budget=budget,
                consume=lambda frame,*rest:rows.extend(frame.collect()))
            chunks.extend(result["chunks"])
        self.assertEqual(self.canonical(rows),self.canonical(self.reference().collect()))
        self.assertEqual(len(chunks),len(set(chunks)));self.assertEqual(len({r.sequence_id for r in rows}),36);self.clean()
    def test_hot_middle_splits_preserve_all_branches(self):
        rows=[];limits=self.lim(max_projected_triples=8,max_middle_expansion=4,max_output_rows=12)
        generate_root_group(self.frame,"HI-Medium",self.plan,limits,range(4),budget=ChunkBudget(limits,lambda:None),
            consume=lambda frame,*rest:rows.extend(frame.collect()))
        self.assertEqual(self.canonical(rows),self.canonical(self.reference().collect()));self.clean()
    def test_hot_a_following_splits_keep_restricted_selectors(self):
        from pyspark.sql import functions as F
        frame=self.frame.where(F.col("txn_id").isin(-9,3,4,5,6))
        limits=self.lim(max_pair_candidates=1,max_pair_links=1,max_projected_triples=1,max_middle_expansion=1,max_output_rows=1)
        rows=[]
        with patch.object(core,"_build_two_hop_links_validated",wraps=core._build_two_hop_links_validated) as joins:
            generate_root_group(frame,"HI-Medium",self.plan,limits,range(4),budget=ChunkBudget(limits,lambda:None),
                consume=lambda output,*rest:rows.extend(output.collect()))
            self.assertTrue(any(call.kwargs["second_txn_ids"] is not None for call in joins.call_args_list))
        self.assertEqual(len(rows),6);self.assertEqual(self.canonical(rows),self.canonical(self.reference(frame).collect()));self.clean()
    def test_guard_and_consumer_failures_cleanup_without_claiming_staging(self):
        limits=self.lim(max_total_output_rows=1);consume=Mock()
        with self.assertRaises(GrowthLimitExceeded):generate_root_group(self.frame,"HI-Medium",self.plan,limits,range(4),
            budget=ChunkBudget(limits,lambda:None),consume=consume)
        consume.assert_not_called();self.clean()
        limits=self.lim()
        with self.assertRaisesRegex(RuntimeError,"sink failed"):generate_root_group(self.frame,"HI-Medium",self.plan,limits,range(4),
            budget=ChunkBudget(limits,lambda:None),consume=Mock(side_effect=RuntimeError("sink failed")))
        self.clean()
    def test_explicit_group_limits_fail_before_output_and_pass_at_equality(self):
        limits=self.lim();consume=Mock()
        for field,value in (("max_group_input_rows",5),("max_group_pair_work",11),("max_group_output_rows",11)):
            with self.assertRaises(GrowthLimitExceeded):generate_root_group(self.frame,"HI-Medium",self.plan,limits,range(4),
                budget=ChunkBudget(limits,lambda:None),consume=consume,**{field:value})
            consume.assert_not_called();self.clean()
        result=generate_root_group(self.frame,"HI-Medium",self.plan,limits,range(4),budget=ChunkBudget(limits,lambda:None),
            consume=lambda *a:None,max_group_input_rows=6,max_group_pair_work=16,max_group_output_rows=36)
        self.assertEqual(result["materialized_pair_work"],16);self.assertEqual(result["reserved_output_rows"],36);self.clean()
    def test_invalid_roots_and_private_selector_cannot_finalize(self):
        limits=self.lim()
        for roots in ([],[0,0],[-1],[4],[True]):
            with self.assertRaises(ValueError):generate_root_group(None,"HI-Medium",self.plan,limits,roots,
                budget=ChunkBudget(limits,lambda:None),consume=Mock())
        with self.assertRaises(ValueError):core.generate_date_chunks(None,"HI-Medium",self.plan,limits,Mock(),
            budget=ChunkBudget(limits,lambda:None),_owner_ids=Mock())

    def test_100000_row_leaves_and_coarser_ownership_without_python_row_collection(self):
        from pyspark.sql import functions as F
        a=(self.spark.range(25000).select((F.lit(-1000000)-F.col("id")).alias("txn_id"))
            .crossJoin(self.frame.where(F.col("txn_id")==-9).drop("txn_id")).select(*CLEANED_SCHEMA.fieldNames()))
        b=(self.spark.range(4).select((F.lit(1000000)+F.col("id")).alias("txn_id"))
            .crossJoin(self.frame.where(F.col("txn_id")==3).drop("txn_id")).select(*CLEANED_SCHEMA.fieldNames()))
        frame=a.unionByName(b).unionByName(self.frame.where(F.col("txn_id")==5))
        limits=self.lim(root_chunks=256,max_preceding_rows=25000,max_pair_candidates=100000,
            max_pair_links=100000,max_projected_triples=100000,max_middle_expansion=100000,max_output_rows=100000,
            max_total_pair_links=100004,max_total_output_rows=200000)
        emitted=[]
        def consume(output,counts,*rest):
            self.assertEqual(len(output.columns),21);emitted.append(dict(counts))
        result=generate_root_group(frame,"HI-Medium",self.plan,limits,range(256),budget=ChunkBudget(limits,lambda:None),
            consume=consume,max_group_input_rows=25000,max_group_pair_work=100004,max_group_output_rows=200000)
        self.assertEqual(emitted,[{2:100000,3:0},{2:0,3:100000}])
        self.assertEqual(sum(result["input_rows"].values()),25000)
        self.assertEqual(sum(v[2]+v[3] for v in result["counts"].values()),200000);self.clean()
        # Also exercise the deployed-style path, without the experimental
        # batching/member-cache adapter. No Parquet/cloud compatibility claim.
        legacy_limits=self.lim(root_chunks=1,max_preceding_rows=25000,max_pair_candidates=100000,
            max_pair_links=100000,max_projected_triples=100000,max_middle_expansion=100000,max_output_rows=100000,
            max_total_pair_links=100004,max_total_output_rows=200000)
        class LegacySink:
            def begin_date(self,*a):pass
            def write_chunk(self,day,root,cid,output,counts,*,ownership):
                if len(output.columns)!=21:raise AssertionError("Changed output contract")
                return dict(chunk_id=cid)
            def complete_root(self,*a):return {}
        legacy=core.generate_date_chunks(frame,"HI-Medium",self.plan,legacy_limits,LegacySink(),
            budget=ChunkBudget(legacy_limits,lambda:None),selected_root=0)
        self.assertEqual(legacy["counts"],{"2":100000,"3":100000});self.clean()


if __name__=="__main__":unittest.main()
