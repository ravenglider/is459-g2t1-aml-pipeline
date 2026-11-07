"""Offline entry-point tests. All SDK/Glue interactions are fakes, not AWS."""
import io
import json
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import glue_sample_generation as entry
from test_sequence_glue_runtime import client_model
from sequence_glue_runtime import GlueCompatibilityError


def argv():
    values = dict(EXPECTED_ACCOUNT_ID="123456789012", REGION="us-east-1", RUN_ID="generation_run_1",
        ATTEMPT_ID="attempt_1",OWNER_TOKEN="owner_1",BUILDER_VERSION="commit_123",BUILDER_FINGERPRINT="a"*64,
        SOURCE_FINGERPRINT="b"*64,TABLE_FINGERPRINT="c"*64,PARTITIONS_FINGERPRINT="d"*64,
        TIMEZONE_EVIDENCE="synthetic UTC confirmation",NO_REFRESH_EVIDENCE="synthetic refresh pause",
        NO_REFRESH_ACKNOWLEDGED="true",ALLOW_AWS_WRITES="true",MAX_PAIR_CANDIDATES="751390",
        MAX_PAIR_LINKS="100000",MAX_PROJECTED_TRIPLES="100000",MAX_TRIPLES_PER_MIDDLE="10000",
        MAX_OUTPUT_ROWS="200000",MAX_RECORDS_PER_FILE="10000")
    values.update({"additional-python-modules":entry.SCRIPT_ROOT+"sample_generation_release_1/role5_sdk.gluewheels.zip",
                   "python-modules-installer-option":"--no-index"})
    return [token for name,value in values.items() for token in ("--"+name,value)]


def changed(name,value=None,remove=False):
    args=argv()
    i=args.index("--"+name)
    if remove:del args[i:i+2]
    else:args[i+1]=value
    return args


def settings():
    return entry.validate_arguments(entry.parse_arguments(argv()))


def dependencies():
    context,catalog,identity = MagicMock(),MagicMock(),MagicMock()
    s3,operation=client_model()
    s3.meta.region_name="us-east-1"
    catalog.meta.region_name=identity.meta.region_name="us-east-1"
    identity.get_caller_identity.return_value={"Account":"123456789012"}
    context.spark_session.version="3.5.4-amzn-0"
    context.spark_session.conf.get.return_value="UTC"
    context.spark_session.sparkContext._jvm.java.lang.System.getProperty.return_value="17.0.20"
    return dict(glue_context=context,catalog_client=catalog,s3_client=s3,identity_client=identity)


def completed():
    return dict(manifest=dict(state="STAGED_VALIDATED",run_id="generation_run_1",attempt_evidence={"counts":{"2":1,"3":1}}),
        manifest_uri="s3://is459-g2t1-aml-processed-bucket/bq2_sequences_metadata/development/generation_run_1/staging_run.json",
        attempt_uri="s3://is459-g2t1-aml-processed-bucket/bq2_sequences_staging/generation_run_1/attempt_1/",published=False)


def fake_modules(deps):
    sdk=MagicMock()
    sdk.Session.return_value.client.side_effect=lambda name,**kwargs:deps[{"s3":"s3_client","glue":"catalog_client","sts":"identity_client"}[name]]
    context_type=MagicMock(return_value=deps["glue_context"])
    job_type=MagicMock()
    spark_type=MagicMock()
    config_type=MagicMock()
    modules={"boto3":sdk,"botocore":types.ModuleType("botocore"),
        "botocore.config":types.SimpleNamespace(Config=config_type),
        "awsglue":types.ModuleType("awsglue"),"awsglue.context":types.SimpleNamespace(GlueContext=context_type),
        "awsglue.job":types.SimpleNamespace(Job=job_type),"pyspark.context":types.SimpleNamespace(SparkContext=spark_type)}
    return modules,sdk,context_type,job_type,spark_type,config_type


class ArgumentTests(unittest.TestCase):
    def test_exact_console_arguments_and_fixed_scope(self):
        args=argv()+["--JOB_NAME",entry.JOB_NAME,"--JOB_RUN_ID","jr_fake","--TempDir",entry.TEMP_ROOT,
            "--extra-py-files",entry.SCRIPT_ROOT+"release_1/role5_modules.zip",
            "--scriptLocation",entry.SCRIPT_ROOT+"release_1/glue_sample_generation.py",
            "--glue-di-packages-correlation-ids","opaque1","--internal-lib-urls","opaque2","--tenant-internal","opaque3",
            "--enable-metrics","true","--job-bookmark-option","job-bookmark-disable"]
        result=entry.validate_arguments(entry.parse_arguments(args))
        self.assertIs(result["allow_writes"],True)
        self.assertEqual(result["config"]["dataset"],"sample")
        self.assertEqual(result["config"]["owner_date"],"2022-09-01")
        self.assertEqual(result["config"]["limits"]["max_pair_candidates"],751390)
        self.assertNotIn("staging_root",result["config"])

    def test_missing_false_malformed_and_duplicate_activation_stop_before_dependencies(self):
        cases=[changed("ALLOW_AWS_WRITES",remove=True),changed("ALLOW_AWS_WRITES","false"),
            changed("ALLOW_AWS_WRITES","TRUE"),changed("ALLOW_AWS_WRITES","1"),
            argv()+["--ALLOW_AWS_WRITES=true"],argv()+["--ALLOW_AWS_WRITES"]]
        with patch.object(entry,"_initialize_dependencies") as initialize,patch("sys.stdout",new_callable=io.StringIO),patch("sys.stderr",new_callable=io.StringIO):
            for args in cases:
                with self.subTest(args=args),self.assertRaises(entry.GenerationArgumentError) as caught:entry.main(args)
                self.assertEqual(caught.exception.code,2)
            initialize.assert_not_called()

    def test_every_required_argument_and_duplicate_configuration_are_rejected(self):
        required=[argv()[i][2:] for i in range(0,len(argv()),2) if argv()[i]!="--ALLOW_AWS_WRITES"]
        with patch.object(entry,"_initialize_dependencies") as initialize,patch("sys.stdout",new_callable=io.StringIO),patch("sys.stderr",new_callable=io.StringIO):
            for name in required:
                with self.subTest(missing=name),self.assertRaises(entry.GenerationArgumentError):entry.main(changed(name,remove=True))
                with self.subTest(duplicate=name),self.assertRaises(entry.GenerationArgumentError):entry.main(argv()+["--"+name,"duplicate"])
            initialize.assert_not_called()

    def test_unknown_paths_test_hooks_dependencies_and_spark_settings_rejected(self):
        for name in ("SOURCE_TABLE","DATABASE","INPUT_DATES","STAGING_ROOT","DESTINATION","SYNTHETIC_BASELINE",
                     "transport","conf","ALLOW_AWS_W","help"):
            with self.subTest(name=name),patch("sys.stdout",new_callable=io.StringIO),patch("sys.stderr",new_callable=io.StringIO),self.assertRaises(entry.GenerationArgumentError):
                entry.parse_arguments(argv()+["--"+name,"private-value"])

    def test_duplicate_runtime_metadata_also_rejected_and_opaque_values_discarded(self):
        for name in ("JOB_RUN_ID","glue-di-packages-correlation-ids","internal-lib-urls","tenant-internal"):
            with self.subTest(name=name),patch("sys.stdout",new_callable=io.StringIO),patch("sys.stderr",new_callable=io.StringIO),self.assertRaises(entry.GenerationArgumentError):
                entry.parse_arguments(argv()+["--"+name,"opaque1","--"+name,"opaque2"])
        parsed=entry.parse_arguments(argv()+["--tenant-internal","private-value"])
        self.assertFalse(hasattr(parsed,"tenant_internal"))

    def test_invalid_config_scope_positive_limits_and_runtime_paths(self):
        cases=[changed("EXPECTED_ACCOUNT_ID","bad"),changed("RUN_ID","../bad"),changed("ATTEMPT_ID","a/b"),
            changed("OWNER_TOKEN",""),changed("BUILDER_VERSION","../version"),changed("SOURCE_FINGERPRINT","bad"),
            changed("TIMEZONE_EVIDENCE",""),changed("NO_REFRESH_ACKNOWLEDGED","false")]
        cases += [argv()+["--DATASET","HI-Medium"],argv()+["--OWNER_DATE","2022-09-02"],changed("REGION","us-west-2")]
        for name in (*entry.LIMITS,"MAX_RECORDS_PER_FILE"):
            for value in ("0","-1","1.2","true"," 2","+2"):cases.append(changed(name,value))
        for name,value in (("TempDir","s3://other/"),("extra-py-files","s3://other/a.zip"),
                ("scriptLocation",entry.SCRIPT_ROOT+"release/glue_sample_preflight.py"),
                ("job-bookmark-option","job-bookmark-enable"),("job-language","scala"),("enable-auto-scaling","true")):
            cases.append(argv()+["--"+name,value])
        with patch.object(entry,"_initialize_dependencies") as initialize,patch("sys.stdout",new_callable=io.StringIO),patch("sys.stderr",new_callable=io.StringIO):
            for args in cases:
                with self.subTest(args=args),self.assertRaises((ValueError,entry.GenerationArgumentError)):entry.main(args)
            initialize.assert_not_called()

    def test_safe_argument_diagnostics_never_dump_values(self):
        secret="private-credential-or-evidence"
        with patch("sys.stdout",new_callable=io.StringIO) as out,patch("sys.stderr",new_callable=io.StringIO) as err:
            with self.assertRaises(entry.GenerationArgumentError) as caught:entry.parse_arguments(argv()+["--UNKNOWN="+secret])
        self.assertNotIn(secret,out.getvalue()+err.getvalue()+str(caught.exception))
        self.assertFalse(caught.exception.diagnostic["values_logged"])

    def test_sdk_bootstrap_arguments_accept_both_forms_and_orders(self):
        names=("additional-python-modules","python-modules-installer-option")
        original=argv()
        base=original[:-4]
        values={name:original[original.index("--"+name)+1] for name in names}
        for order in (names,tuple(reversed(names))):
            for inline_sdk in (False,True):
                for inline_installer in (False,True):
                    args=list(base)
                    for name in order:
                        inline=inline_sdk if name==names[0] else inline_installer
                        args.extend(["--"+name+"="+values[name]] if inline else ["--"+name,values[name]])
                    with self.subTest(order=order,sdk=inline_sdk,installer=inline_installer):
                        parsed=entry.parse_arguments(args)
                        self.assertEqual(parsed.additional_python_modules,values[names[0]])
                        self.assertEqual(parsed.python_modules_installer_option,"--no-index")
                        self.assertEqual(entry.validate_arguments(parsed),settings())
                        # argv=None must use the same narrow normalization as explicit argv.
                        with patch("sys.argv",["glue_sample_generation.py",*args]):
                            self.assertEqual(entry.validate_arguments(entry.parse_arguments()),settings())

    def test_sdk_missing_duplicate_and_malformed_arguments_never_create_dependencies(self):
        sdk="additional-python-modules"
        installer="python-modules-installer-option"
        cases=[changed(sdk,remove=True),changed(installer,remove=True),argv()[:-4]]
        for name in (sdk,installer):
            # Missing value, spaced duplicate and mixed-form duplicate.
            cases.extend([changed(name,remove=True)+["--"+name],
                argv()+["--"+name,argv()[argv().index("--"+name)+1]],
                argv()+["--"+name+"="+argv()[argv().index("--"+name)+1]]])
        for value in ("", "--index-url", "--no-index=true", "--no-deps", "--no-index --no-deps", " --no-index"):
            cases.append(changed(installer,value))
        for suffix in ("release/role5_modules.zip","release/role5_sdk.zip","release/role5_sdk.gluewheels.zip/",
                       "../role5_sdk.gluewheels.zip","release/sub/role5_sdk.gluewheels.zip",
                       "%72elease/role5_sdk.gluewheels.zip","release/role5_sdk.gluewheels.zip?token=private",
                       "release/role5_sdk.gluewheels.zip#fragment","release//role5_sdk.gluewheels.zip",
                       "release.name/role5_sdk.gluewheels.zip", "a"*65+"/role5_sdk.gluewheels.zip"):
            cases.append(changed(sdk,entry.SCRIPT_ROOT+suffix))
        for value in ("boto3==1.35.93", "s3://other/scripts/role5/release/role5_sdk.gluewheels.zip",
                      "https://is459-g2t1-aml-athena-bucket/scripts/role5/release/role5_sdk.gluewheels.zip",
                      entry.SCRIPT_ROOT+"release/role5_sdk.gluewheels.zip,boto3==1.35.93",
                      " "+entry.SCRIPT_ROOT+"release/role5_sdk.gluewheels.zip",
                      entry.SCRIPT_ROOT+"release/role5_sdk.gluewheels.zip\n"):
            cases.append(changed(sdk,value))
        cases.extend([argv()+["--conf","spark.any=value"],argv()+["--no-index"],
                      argv()+["--python-modules-installer-options=--no-index"],
                      argv()+["--extra-py-files",entry.SCRIPT_ROOT+"release/role5_sdk.gluewheels.zip"],
                      argv()+["--extra-py-files",entry.SCRIPT_ROOT+"release/role5_modules.zip,other.zip"],
                      changed(installer,remove=True)+["--python-modules-installer-option","--UNKNOWN=value"]])
        deps=dependencies()
        modules,sdk_module,context_type,job_type,spark_type,_=fake_modules(deps)
        with patch.dict(sys.modules,modules),patch.object(entry,"_initialize_dependencies") as initialize, \
             patch("sys.stdout",new_callable=io.StringIO),patch("sys.stderr",new_callable=io.StringIO):
            for args in cases:
                with self.subTest(args=args),self.assertRaises((ValueError,entry.GenerationArgumentError)):
                    entry.main(args)
            initialize.assert_not_called()
        sdk_module.Session.assert_not_called()
        context_type.assert_not_called()
        spark_type.getOrCreate.assert_not_called()
        job_type.assert_not_called()

    def test_sdk_values_stay_out_of_safe_argument_diagnostics(self):
        secret="private-sdk-configuration"
        cases=[changed("python-modules-installer-option",secret),
               argv()+["--additional-python-modules="+secret],
               changed("additional-python-modules",entry.SCRIPT_ROOT+secret+"/wrong.zip")]
        with patch("sys.stdout",new_callable=io.StringIO) as out,patch("sys.stderr",new_callable=io.StringIO) as err:
            for args in cases:
                with self.assertRaises((ValueError,entry.GenerationArgumentError)) as caught:
                    entry.main(args)
                self.assertNotIn(secret,str(caught.exception))
        self.assertNotIn(secret,out.getvalue()+err.getvalue())

    def test_unknown_categories_and_positions_fail_before_dependencies(self):
        cases=(("", "empty"), ("-private-short", "single_dash_token"), ("true", "boolean_literal"),
               ("false", "boolean_literal"), ("TRUE", "boolean_literal"),
               ("/private/path/glue_sample_generation.py", "entry_script_basename_match"),
               (r"C:\private\glue_sample_generation.py", "entry_script_basename_match"),
               ("glue_sample_generation.py", "entry_script_basename_match"),
               ("private-evidence-text", "other_positional"), ("--private-option=private-value", "unknown_long_option"))
        base=argv()+["--job-language","python"]
        with patch.object(entry,"_initialize_dependencies") as initialize, \
             patch("sys.stdout",new_callable=io.StringIO),patch("sys.stderr",new_callable=io.StringIO):
            for token,category in cases:
                for before in (True,False):
                    args=[token,*base] if before else [*base,token]
                    with self.subTest(category=category,before=before),self.assertRaises(entry.GenerationArgumentError) as caught:
                        entry.main(args)
                    diagnostic=caught.exception.diagnostic
                    self.assertEqual(caught.exception.code,2)
                    self.assertEqual(diagnostic["reason"],"unrecognized_arguments")
                    self.assertEqual(diagnostic["unparsed_count"],1)
                    self.assertEqual(diagnostic["arguments"],[])
                    self.assertEqual(diagnostic["unrecognized_token_summary"],[dict(category=category,
                        position="before_first_recognized_option" if before else "after_first_recognized_option",count=1)])
                    self.assertFalse(diagnostic["values_logged"])
                    self.assertFalse(diagnostic["generation_completed"])
            initialize.assert_not_called()

    def test_repeated_values_keep_correct_positions_and_diagnostics_are_bounded(self):
        # Identical strings may be both consumed values and extras; .index(value)
        # would misreport positions. Prefix and later extras must stay distinct.
        base=argv()+["--enable-metrics","true","--job-language","python"]
        args=["","true",*base,"","true"]
        with patch("sys.stdout",new_callable=io.StringIO),patch("sys.stderr",new_callable=io.StringIO):
            with self.assertRaises(entry.GenerationArgumentError) as caught:entry.parse_arguments(args)
            summary=caught.exception.diagnostic["unrecognized_token_summary"]
            self.assertEqual(summary,[dict(category=c,position=p,count=1) for c in ("empty","boolean_literal")
                for p in ("before_first_recognized_option","after_first_recognized_option")])
            repeated=["private-positional"]*1000
            with self.assertRaises(entry.GenerationArgumentError) as caught:entry.parse_arguments([*repeated,*base,*repeated])
        diagnostic=caught.exception.diagnostic
        self.assertEqual(diagnostic["unparsed_count"],2000)
        self.assertEqual(sum(r["count"] for r in diagnostic["unrecognized_token_summary"]),2000)
        self.assertEqual(len(diagnostic["unrecognized_token_summary"]),2)
        self.assertLess(len(str(caught.exception)),2048)

    def test_diagnostic_summary_never_exposes_unknown_names_or_private_values(self):
        secrets=("123456789012", "a"*64, "AKIA1234567890ABCDEF", "/private/customer/location", "private-evidence")
        tokens=[*secrets,"--private-prefix-"+secrets[0],"--opaque="+secrets[-1],
                "/private/customer/glue_sample_generation.py"]
        with patch("sys.stdout",new_callable=io.StringIO) as out,patch("sys.stderr",new_callable=io.StringIO) as err:
            with self.assertRaises(entry.GenerationArgumentError) as caught:entry.parse_arguments([*tokens,*argv()])
        rendered=out.getvalue()+err.getvalue()+str(caught.exception)
        for secret in (*secrets,"--private-prefix","--opaque","glue_sample_generation.py"):
            self.assertNotIn(secret,rendered)
        self.assertEqual(out.getvalue(),err.getvalue())
        diagnostic=caught.exception.diagnostic
        self.assertEqual(diagnostic["arguments"],[])
        self.assertEqual(sum(r["count"] for r in diagnostic["unrecognized_token_summary"]),len(tokens))

    def test_metrics_forms_and_installer_forms_preserve_valid_settings(self):
        for metrics in (["--enable-metrics"],["--enable-metrics","true"],
                        ["--enable-metrics=true"],["--enable-metrics",""]):
            for inline_installer in (False,True):
                args=argv()
                if inline_installer:
                    i=args.index("--python-modules-installer-option")
                    args[i:i+2]=["--python-modules-installer-option=--no-index"]
                args+=metrics
                with self.subTest(metrics=metrics,inline_installer=inline_installer):
                    self.assertEqual(entry.validate_arguments(entry.parse_arguments(args)),settings())
                    with patch("sys.argv",["glue_sample_generation.py",*args]):
                        self.assertEqual(entry.validate_arguments(entry.parse_arguments()),settings())

    def test_duplicate_diagnostics_remain_known_names_without_unknown_summary(self):
        cases=(argv()+["--enable-metrics","true","--enable-metrics=true"],argv()+["--ALLOW_AWS_WRITES=true"])
        with patch.object(entry,"_initialize_dependencies") as initialize, \
             patch("sys.stdout",new_callable=io.StringIO),patch("sys.stderr",new_callable=io.StringIO):
            for args in cases:
                with self.assertRaises(entry.GenerationArgumentError) as caught:entry.main(args)
                diagnostic=caught.exception.diagnostic
                self.assertEqual(diagnostic["reason"],"duplicate_argument")
                self.assertEqual(diagnostic["unparsed_count"],0)
                self.assertNotIn("unrecognized_token_summary",diagnostic)
            initialize.assert_not_called()


class RuntimeTests(unittest.TestCase):
    def test_checks_precede_coordinator_and_exact_settings_are_forwarded(self):
        deps=dependencies()
        events=[]
        deps["identity_client"].get_caller_identity.side_effect=lambda:events.append("account") or {"Account":"123456789012"}
        model=deps["s3_client"].meta.service_model.operation_model
        operation=model.return_value
        model.side_effect=lambda name:events.append("sdk") or operation
        with patch("sequence_sample_run.coordinate_staging_run",side_effect=lambda **kwargs:events.append("coordinate") or completed()) as coordinate:
            result=entry.run_generation(settings(),**deps)
        self.assertEqual(events,["account","sdk","coordinate"])
        self.assertEqual(result["runtime"]["spark_version"],"3.5.4-amzn-0")
        self.assertEqual(coordinate.call_args.kwargs,dict(settings(),**deps))
        deps["s3_client"].put_object.assert_not_called()

    def test_account_region_sdk_runtime_and_timezone_fail_before_coordinator(self):
        for mode in ("account","region","sdk","spark","java","python","timezone","activation"):
            deps=dependencies()
            values=settings()
            if mode=="account":deps["identity_client"].get_caller_identity.return_value={"Account":"999999999999"}
            elif mode=="region":deps["catalog_client"].meta.region_name="us-west-2"
            elif mode=="sdk":deps["s3_client"].meta.service_model.operation_model.return_value.input_shape.members={}
            elif mode=="spark":deps["glue_context"].spark_session.version="3.5.4-amzn-1"
            elif mode=="java":deps["glue_context"].spark_session.sparkContext._jvm.java.lang.System.getProperty.return_value="21.0.1"
            elif mode=="timezone":deps["glue_context"].spark_session.conf.get.return_value="Asia/Singapore"
            elif mode=="activation":values["allow_writes"]=False
            python=(3,12,0) if mode=="python" else (3,11,15)
            with self.subTest(mode=mode),patch("sequence_glue_runtime.sys.version_info",python),patch("sequence_sample_run.coordinate_staging_run") as coordinate:
                with self.assertRaises(ValueError):entry.run_generation(values,**deps)
                coordinate.assert_not_called()
                deps["s3_client"].put_object.assert_not_called()

    def test_dependency_construction_model_gate_before_spark_and_fixed_sdk_settings(self):
        deps=dependencies()
        modules,sdk,context_type,job_type,spark_type,config_type=fake_modules(deps)
        with patch.dict(sys.modules,modules):
            result,job=entry._initialize_dependencies(settings())
        self.assertEqual(result,deps)
        config_type.assert_called_once_with(connect_timeout=10,read_timeout=60,retries={"total_max_attempts":1})
        sdk.Session.assert_called_once_with(region_name="us-east-1")
        spark_type.getOrCreate.assert_called_once()
        job.init.assert_called_once_with(entry.JOB_NAME,{"JOB_NAME":entry.JOB_NAME,"DATASET":"sample"})
        for name in ("s3_client","catalog_client","identity_client"):self.assertIs(result[name],deps[name])
        for mode in ("account","sdk","spark"):
            deps=dependencies()
            if mode=="account":deps["identity_client"].get_caller_identity.return_value={"Account":"999999999999"}
            elif mode=="sdk":deps["s3_client"].meta.service_model.operation_model.return_value.input_shape.members={}
            else:deps["glue_context"].spark_session.version="4.0.0"
            modules,_,context_type,job_type,spark_type,_=fake_modules(deps)
            with self.subTest(mode=mode),patch.dict(sys.modules,modules),self.assertRaises(ValueError):entry._initialize_dependencies(settings())
            if mode=="spark":
                spark_type.getOrCreate.assert_called_once()
                context_type.assert_called_once()
            else:
                spark_type.getOrCreate.assert_not_called()
                context_type.assert_not_called()
            job_type.assert_not_called()

    def test_nonterminal_or_publication_claim_is_not_success(self):
        for state,published in (("STAGING",False),("STAGED_VALIDATED",True)):
            result=completed()
            result["manifest"]["state"]=state
            result["published"]=published
            with self.subTest(state=state,published=published),patch(
                    "sequence_sample_run.coordinate_staging_run",return_value=result),self.assertRaises(ValueError):
                entry.run_generation(settings(),**dependencies())

    def test_main_commit_and_safe_failure_reporting(self):
        deps=dependencies()
        job=MagicMock()
        with patch.object(entry,"_initialize_dependencies",return_value=(deps,job)),patch("sequence_sample_run.coordinate_staging_run",return_value=completed()),patch("sys.stdout",new_callable=io.StringIO) as out:
            self.assertEqual(entry.main(argv()),0)
        job.commit.assert_called_once()
        self.assertIn('"status": "STAGED_VALIDATED"',out.getvalue())
        secret="private-failure-value"
        for failure in (RuntimeError(secret),GlueCompatibilityError("unsupported_conditional_writes",missing_capabilities=["IfMatch"])):
            with patch.object(entry,"_initialize_dependencies",side_effect=failure),patch("sys.stdout",new_callable=io.StringIO) as out:
                with self.assertRaises(type(failure)) as caught:entry.main(argv())
                self.assertIs(caught.exception,failure)
                self.assertNotIn(secret,out.getvalue())
                self.assertIn('"status": "FAILED"',out.getvalue())

    def test_commit_failure_is_not_logged_as_success(self):
        deps=dependencies()
        job=MagicMock()
        job.commit.side_effect=RuntimeError("commit failed")
        with patch.object(entry,"_initialize_dependencies",return_value=(deps,job)),patch("sequence_sample_run.coordinate_staging_run",return_value=completed()),patch("sys.stdout",new_callable=io.StringIO) as out:
            with self.assertRaisesRegex(RuntimeError,"commit failed"):entry.main(argv())
        self.assertNotIn('"status": "STAGED_VALIDATED"',out.getvalue())


class ScriptTests(unittest.TestCase):
    def test_actual_main_script_success_and_coordinator_exception(self):
        for failure in (None,RuntimeError("coordinator failed")):
            deps=dependencies()
            modules,_,_,job_type,_,_=fake_modules(deps)
            with patch.dict(sys.modules,modules),patch("sys.argv",[str(ROOT/"glue_sample_generation.py"),*argv()]),patch(
                    "sequence_sample_run.coordinate_staging_run",return_value=completed(),side_effect=failure),patch("sys.stdout",new_callable=io.StringIO):
                if failure:
                    with self.assertRaisesRegex(RuntimeError,"coordinator failed"):runpy.run_path(str(ROOT/"glue_sample_generation.py"),run_name="__main__")
                    job_type.return_value.commit.assert_not_called()
                else:
                    runpy.run_path(str(ROOT/"glue_sample_generation.py"),run_name="__main__")
                    job_type.return_value.commit.assert_called_once()

    def test_actual_main_missing_activation_exits_nonzero_without_sdk(self):
        with patch("sys.argv",[str(ROOT/"glue_sample_generation.py"),*changed("ALLOW_AWS_WRITES",remove=True)]),patch.dict(sys.modules,{"boto3":None}):
            with self.assertRaises(SystemExit) as caught:runpy.run_path(str(ROOT/"glue_sample_generation.py"),run_name="__main__")
        self.assertEqual(caught.exception.code,2)

    def test_entry_script_imports_with_only_generation_zip_and_no_aws_imports(self):
        from package_glue_modules import build_package,GENERATION_MODULES
        script="""import importlib.abc,importlib.util,sys
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname.split('.')[0] in {'boto3','botocore','awsglue','pyspark'}: raise AssertionError(fullname)
sys.meta_path.insert(0,Block())
sys.path.insert(0,sys.argv[1])
spec=importlib.util.spec_from_file_location('release_entry',sys.argv[2])
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
assert 'pyspark' not in sys.modules
assert sys.modules['glue_sample_preflight'].__file__.startswith(sys.argv[1])
"""
        with tempfile.TemporaryDirectory() as folder:
            archive=Path(folder)/"role5_modules.zip"
            build_package(archive,profile="generation")
            copied=Path(folder)/"glue_sample_generation.py"
            copied.write_bytes((ROOT/"glue_sample_generation.py").read_bytes())
            result=subprocess.run([sys.executable,"-I","-c",script,str(archive),str(copied)],cwd=folder,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertNotIn("glue_sample_generation.py",GENERATION_MODULES)


if __name__=="__main__":unittest.main()
