"""Pure offline harness tests; installing/downloading is never a test side effect."""

import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import verify_glue_sdk as verifier


class SDKHarnessTests(unittest.TestCase):
    def test_candidate_exact_pins_include_glue_filesystem_constraints(self):
        values = verifier.pins(ROOT / "requirements_sdk_candidate.txt")
        self.assertEqual(len(values), 20)
        self.assertEqual(values["boto3"], "1.35.93")
        self.assertEqual(values["botocore"], "1.35.93")
        self.assertEqual(values["aiobotocore"], "2.17.0")
        self.assertEqual(values["s3transfer"], "0.10.4")
        self.assertEqual(values["s3fs"], values["fsspec"])
        self.assertEqual(values["s3fs"], "2024.6.1")

    def test_reject_loose_duplicate_and_empty_requirements(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "requirements.txt"
            for text in ("", "boto3>=1.0", "boto3==1.0\nboto3==1.0", "-r other.txt"):
                path.write_text(text, encoding="utf-8")
                with self.assertRaises(ValueError):
                    verifier.pins(path)

    def test_project_environment_is_not_disposable_target(self):
        with self.assertRaises(ValueError):
            verifier.check_disposable(ROOT / ".venv")

    def wheel(self, folder, version="1.0"):
        path = Path(folder) / f"example-{version}-py3-none-any.whl"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr(f"example-{version}.dist-info/METADATA", f"Name: example\nVersion: {version}\n")
        return path

    def test_wheel_inventory_hash_and_missing_extra_rejection(self):
        with tempfile.TemporaryDirectory() as folder:
            path = self.wheel(folder)
            records = verifier.wheel_records(folder, {"example": "1.0"})
            self.assertEqual(records[0]["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
            for expected in ({}, {"example": "2.0"}, {"example": "1.0", "missing": "1.0"}):
                with self.assertRaises(ValueError):
                    verifier.wheel_records(folder, expected)

    def test_duplicate_wheels_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = self.wheel(folder)
            (Path(folder) / "duplicate.whl").write_bytes(path.read_bytes())
            with self.assertRaises(ValueError):
                verifier.wheel_records(folder, {"example": "1.0"})

    def test_provenance_creates_hash_lock_only_after_verification(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            path = self.wheel(folder)
            requirements = root / "requirements.txt"
            requirements.write_text("example==1.0", encoding="utf-8")
            item = dict(filename=path.name, digests={"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
                        url="https://files.pythonhosted.org/packages/example.whl")
            with patch.object(verifier.urllib.request, "urlopen", return_value=io.BytesIO(json.dumps({"urls": [item]}).encode())) as fetch:
                verifier.provenance(folder, requirements, root / "provenance.json", root / "lock.txt")
            self.assertEqual(fetch.call_args.args[0], "https://pypi.org/pypi/example/1.0/json")
            self.assertIn("--hash=sha256:" + item["digests"]["sha256"], (root / "lock.txt").read_text())

    def test_bad_provenance_hash_or_host_produces_no_lock(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            path = self.wheel(folder)
            requirements = root / "requirements.txt"
            requirements.write_text("example==1.0", encoding="utf-8")
            for digest, url in (("0" * 64, "https://files.pythonhosted.org/a"),
                                (hashlib.sha256(path.read_bytes()).hexdigest(), "https://untrusted.example/a")):
                item = dict(filename=path.name, digests={"sha256": digest}, url=url)
                with patch.object(verifier.urllib.request, "urlopen", return_value=io.BytesIO(json.dumps({"urls": [item]}).encode())):
                    with self.assertRaises(ValueError):
                        verifier.provenance(folder, requirements, root / "evidence.json", root / "lock.txt")
                self.assertFalse((root / "lock.txt").exists())

    def test_serializer_probe_checks_both_headers_without_client(self):
        calls = []
        class Serializer:
            def serialize_to_request(self, parameters, operation):
                calls.append(parameters)
                headers = {"If-None-Match" if key == "IfNoneMatch" else "If-Match": value
                           for key, value in parameters.items() if key in ("IfNoneMatch", "IfMatch")}
                return {"headers": headers}
        string = SimpleNamespace(type_name="string")
        operation = SimpleNamespace(name="PutObject", input_shape=SimpleNamespace(
            type_name="structure", members={"IfMatch": string, "IfNoneMatch": string}))
        service = SimpleNamespace(service_name="s3", operation_model=lambda name: operation,
                                  metadata={"protocol": "rest-xml"})
        fake = SimpleNamespace(create_serializer=lambda protocol: Serializer())
        with patch.dict(sys.modules, {"botocore.serialize": fake}):
            result = verifier.probe_model(service)
        self.assertEqual(result["serialized_headers"], {"If-None-Match": "*", "If-Match": '"offline-etag"'})
        self.assertEqual(result["requests_sent"], 0)
        self.assertEqual(len(calls), 2)
        with patch.dict(sys.modules, {"botocore.serialize": fake}), \
             patch.object(Serializer, "serialize_to_request", return_value={"headers": {}}), \
             self.assertRaises(ValueError):
            verifier.probe_model(service)


if __name__ == "__main__":
    unittest.main()
