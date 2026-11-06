"""Pure runtime/model checks; no SDK, credentials, Spark or network required."""

import io
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import sequence_glue_runtime as runtime


def client_model(parameters=runtime.CONDITIONAL_PARAMETERS):
    members = {name: SimpleNamespace(type_name="string") for name in parameters}
    shape = SimpleNamespace(type_name="structure", members=members)
    operation = SimpleNamespace(name="PutObject", input_shape=shape)
    service = SimpleNamespace(service_name="s3", operation_model=Mock(return_value=operation))
    client = SimpleNamespace(meta=SimpleNamespace(service_model=service),
                             put_object=Mock(side_effect=AssertionError("must never write")),
                             get_object=Mock(side_effect=AssertionError("must never read")))
    return client, operation


class RuntimeTests(unittest.TestCase):
    def check(self, **changes):
        values = dict(glue_version="5.0", spark_version="3.5.4", java_version="17.0.20")
        values.update(changes)
        return runtime.validate_glue_runtime(**values)

    def test_both_approved_spark_builds_and_java_17_formats(self):
        with patch.object(runtime.sys, "version_info", (3, 11, 15)):
            for spark in runtime.SPARK_VERSIONS:
                for java in ("17", "17.0.20", "17.0.12+7-LTS", "17.0.20-amzn-1"):
                    with self.subTest(spark=spark, java=java):
                        report = self.check(spark_version=spark, java_version=java)
                        self.assertEqual(report["status"], "COMPATIBLE")
                        self.assertTrue(all(report["checks"].values()))

    def test_near_matches_wrong_versions_and_types_fail_closed(self):
        cases = dict(glue_version=("4.0", "5.1", 5.0, None, "5.0 "),
                     spark_version=("3.5.4-amzn-1", "3.5.5", "3.5.4-custom", "4.0.0", "3.5.4 ", None),
                     java_version=("8.0.1", "21.0.1", "170", "17.0.20 ", None, 17))
        with patch.object(runtime.sys, "version_info", (3, 11, 15)):
            for field, values in cases.items():
                for value in values:
                    with self.subTest(field=field, value=value), self.assertRaises(runtime.GlueCompatibilityError):
                        self.check(**{field: value})

    def test_actual_python_major_minor_is_required(self):
        for version in ((3, 10, 15), (3, 12, 5), (2, 7, 18)):
            with patch.object(runtime.sys, "version_info", version), self.assertRaises(runtime.GlueCompatibilityError) as caught:
                self.check()
            self.assertEqual(caught.exception.diagnostic["failed_requirements"], ["python"])

    def test_runtime_errors_identify_all_failed_requirements_without_values(self):
        secret = "private-configuration-value"
        with patch.object(runtime.sys, "version_info", (3, 12, 0)), self.assertRaises(runtime.GlueCompatibilityError) as caught:
            self.check(glue_version=secret, spark_version=secret, java_version=secret)
        self.assertEqual(caught.exception.diagnostic["failed_requirements"], ["glue", "spark", "python", "java"])
        self.assertNotIn(secret, str(caught.exception))


class ConditionalModelTests(unittest.TestCase):
    def test_supported_model_no_io_or_mutation(self):
        client, operation = client_model()
        keys = list(operation.input_shape.members)
        report = runtime.validate_s3_conditional_writes(client)
        self.assertTrue(report["conditional_writes_supported"])
        client.meta.service_model.operation_model.assert_called_once_with("PutObject")
        self.assertEqual(list(operation.input_shape.members), keys)
        client.put_object.assert_not_called()
        client.get_object.assert_not_called()

    def test_historical_glue5_model_without_either_put_condition_fails(self):
        # Represents PutObject's old shape, not GetObject's conditional support.
        client, _ = client_model(("Bucket", "Key", "Body", "ContentType"))
        with self.assertRaises(runtime.GlueCompatibilityError) as caught:
            runtime.validate_s3_conditional_writes(client)
        self.assertEqual(caught.exception.diagnostic["missing_capabilities"], ["IfNoneMatch", "IfMatch"])
        client.put_object.assert_not_called()

    def test_each_missing_parameter_is_named(self):
        for missing in runtime.CONDITIONAL_PARAMETERS:
            client, _ = client_model(name for name in runtime.CONDITIONAL_PARAMETERS if name != missing)
            with self.subTest(missing=missing), self.assertRaises(runtime.GlueCompatibilityError) as caught:
                runtime.validate_s3_conditional_writes(client)
            self.assertEqual(caught.exception.diagnostic["missing_capabilities"], [missing])

    def test_conditional_member_must_be_a_string_shape(self):
        for name in runtime.CONDITIONAL_PARAMETERS:
            client, operation = client_model()
            operation.input_shape.members[name].type_name = "boolean"
            with self.subTest(name=name), self.assertRaises(runtime.GlueCompatibilityError) as caught:
                runtime.validate_s3_conditional_writes(client)
            self.assertEqual(caught.exception.diagnostic["invalid_capabilities"], [name])

    def test_malformed_clients_models_and_input_shapes_rejected(self):
        clients = [None, object(), SimpleNamespace(meta=None)]
        for mutation in ("wrong_service", "no_operation", "wrong_operation", "no_input", "not_structure", "no_members", "bad_members", "bad_member"):
            client, operation = client_model()
            if mutation == "wrong_service": client.meta.service_model.service_name = "glue"
            elif mutation == "no_operation": client.meta.service_model.operation_model.return_value = None
            elif mutation == "wrong_operation": operation.name = "GetObject"
            elif mutation == "no_input": operation.input_shape = None
            elif mutation == "not_structure": operation.input_shape.type_name = "list"
            elif mutation == "no_members": del operation.input_shape.members
            elif mutation == "bad_members": operation.input_shape.members = ["IfMatch", "IfNoneMatch"]
            elif mutation == "bad_member": operation.input_shape.members["IfMatch"] = None
            clients.append(client)
        for client in clients:
            with self.subTest(client_type=type(client).__name__), self.assertRaises(runtime.GlueCompatibilityError) as caught:
                runtime.validate_s3_conditional_writes(client)
            self.assertEqual(caught.exception.diagnostic["reason"], "malformed_s3_operation_model")

    def test_sdk_version_labels_do_not_override_operation_model(self):
        supported, _ = client_model()
        supported.sdk_version = "1.34.131"
        self.assertTrue(runtime.validate_s3_conditional_writes(supported)["conditional_writes_supported"])
        unsupported, _ = client_model(())
        unsupported.sdk_version = "999.0.0"
        with self.assertRaises(runtime.GlueCompatibilityError):
            runtime.validate_s3_conditional_writes(unsupported)

    def test_exception_and_unrelated_members_never_leak_sensitive_values(self):
        import traceback
        secret = "credential-or-private-endpoint"
        client, _ = client_model()
        client.meta.service_model.operation_model.side_effect = RuntimeError(secret)
        with patch("sys.stdout", new_callable=io.StringIO) as out, patch("sys.stderr", new_callable=io.StringIO) as err:
            try:
                runtime.validate_s3_conditional_writes(client)
            except runtime.GlueCompatibilityError as caught:
                rendered = "".join(traceback.format_exception(type(caught), caught, caught.__traceback__))
                self.assertNotIn(secret, rendered)
                self.assertFalse(caught.diagnostic["values_logged"])
            else:
                self.fail("SDK metadata exception was swallowed")
        self.assertEqual(out.getvalue()+err.getvalue(), "")
        client, _ = client_model((secret,))
        with self.assertRaises(runtime.GlueCompatibilityError) as caught:
            runtime.validate_s3_conditional_writes(client)
        self.assertNotIn(secret, str(caught.exception))

    def test_import_and_validation_need_no_sdk_spark_network_or_environment(self):
        code = """import importlib.abc, sys, socket, os
from unittest.mock import patch
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self, fullname, path=None, target=None):
  if fullname.split('.')[0] in {'boto3','botocore','pyspark','awsglue'}: raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
sys.path.insert(0, sys.argv[1])
with patch.object(socket, 'socket', side_effect=AssertionError('network forbidden')), patch.object(os, 'getenv', side_effect=AssertionError('environment forbidden')):
 import sequence_glue_runtime as m
 from types import SimpleNamespace as S
 shape=S(type_name='structure', members={n:S(type_name='string') for n in m.CONDITIONAL_PARAMETERS})
 operation=S(name='PutObject', input_shape=shape)
 service=S(service_name='s3', operation_model=lambda name: operation)
 assert m.validate_s3_conditional_writes(S(meta=S(service_model=service)))['conditional_writes_supported']
 with patch.object(m.sys, 'version_info', (3,11,15)):
  assert m.validate_glue_runtime(glue_version='5.0', spark_version='3.5.4-amzn-0', java_version='17.0.20')['status']=='COMPATIBLE'
"""
        result = subprocess.run([sys.executable, "-I", "-c", code, str(Path(runtime.__file__).parent)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
