"""Synthetic SDK evidence and injected read-only Git responses; never Git commits.

Real packagers/validators operate on small fixture wheels and current source
bytes. These tests do not authenticate the actual verified Linux SDK bundle.
"""

import hashlib
import json
from pathlib import Path
import socket
import stat
import sys
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import assemble_generation_release as release
import test_package_glue_sdk as sdk_fixtures


class GenerationReleaseTests(unittest.TestCase):
    def setUp(self):
        self.fixture = sdk_fixtures.SDKBundleTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        self.repo = self.root / "repository"
        self.repo.mkdir()
        self.releases = self.root / "releases"
        self.releases.mkdir()
        self.revision = "a" * 40
        self.head = self.revision
        self.dirty = b""
        names = set(release.application.GENERATION_MODULES) | {"glue_sample_generation.py", "package_glue_modules.py",
            "package_glue_sdk.py", "verify_glue_sdk.py", "requirements_sdk_candidate.txt"}
        self.source = {name: (ROOT / name).read_text(encoding="utf-8").replace("\r\n", "\n").encode() for name in names}
        self.modes = {name: "100644" for name in names}
        self.sdk_hash = self.fixture.build()
        self.bundle = self.fixture.output
        self.app_hash = release.application.build_package(self.root / "verified_application.zip", profile="generation")
        self.report = dict(environment=dict(interpreter="/temporary/venv/bin/python", python="3.11.18 (synthetic)",
            prefix="/temporary/venv", disposable_workspace="/temporary"),
            platform="linux", installed=self.fixture.approved, wheels=list(self.fixture.records.values()),
            pip_check="No broken requirements found.", pip_version="24.0",
            model=dict(capability=dict(status="COMPATIBLE", operation="PutObject", conditional_writes_supported=True,
                required_capabilities=["IfNoneMatch", "IfMatch"], missing_capabilities=[]),
                serialized_headers={"If-None-Match": "*", "If-Match": '"offline-etag"'}, requests_sent=0),
            zip_imports="13 generation modules imported; no network/client/Spark startup",
            requirements_sha256=release._sha(self.source["requirements_sdk_candidate.txt"]),
            application_zip_sha256=self.app_hash, limitation="Synthetic fixture only")
        self.verification = self.root / "verification.json"
        self.save_report()
        self.git_patch = patch.object(release, "_git", side_effect=self.git)
        self.mock_git = self.git_patch.start()
        self.addCleanup(self.git_patch.stop)
        self.hash_patch = patch.object(release, "VERIFIED_SDK_SHA256", self.sdk_hash)
        self.hash_patch.start()
        self.addCleanup(self.hash_patch.stop)

    def save_report(self):
        self.verification.write_text(json.dumps(self.report), encoding="utf-8")

    def git(self, repo, *args):
        self.assertEqual(repo, self.repo)
        if args[0] == "status":
            return self.dirty
        if args[:2] == ("rev-parse", "--show-toplevel"):
            return str(self.repo).encode() + b"\n"
        if args[:2] == ("rev-parse", "--verify"):
            return self.head.encode() + b"\n"
        if args[0] == "ls-tree":
            return b"".join(f"{self.modes[n]} blob {'b'*40}\t{release.PREFIX+n}\0".encode() for n in sorted(self.source))
        if args[0] == "show":
            return self.source[args[1].split(":", 1)[1].removeprefix(release.PREFIX)]
        raise AssertionError("Unexpected Git command: " + str(args))

    def assemble(self, **changes):
        arguments = dict(output_root=self.releases, release_id="sample_generation_20261106_01", repo=self.repo,
            revision=self.revision, sdk_bundle=self.bundle, lock=self.fixture.lock,
            provenance=self.fixture.provenance, verification=self.verification)
        arguments.update(changes)
        return release.assemble_release(**arguments)

    def reject(self, **changes):
        with self.assertRaises((ValueError, FileNotFoundError, zipfile.BadZipFile)):
            self.assemble(**changes)
        self.assertEqual(list(self.releases.iterdir()), [])

    def test_reproducible_complete_layout_hashes_and_builder_arguments(self):
        first = self.assemble()
        alternate = self.root / "alternate"
        alternate.mkdir()
        second = self.assemble(output_root=alternate)
        self.assertEqual(first, second)
        a, b = self.releases / first["release_id"], alternate / first["release_id"]
        files = {"glue_sample_generation.py", "role5_modules.zip", "role5_sdk.gluewheels.zip",
                 "evidence/release_manifest.json", "evidence/sdk-linux.lock.txt", "evidence/sdk-provenance.json",
                 "evidence/sdk-verification.json"}
        self.assertEqual({p.relative_to(a).as_posix() for p in a.rglob("*") if p.is_file()}, files)
        for filename in files:
            self.assertEqual((a / filename).read_bytes(), (b / filename).read_bytes())
        for filename, record in first["artifacts"].items():
            payload = (a / filename).read_bytes()
            self.assertEqual(record, dict(filename=filename, size_bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest()))
        self.assertEqual(first["builder_fingerprint"], first["artifacts"]["role5_modules.zip"]["sha256"])
        self.assertEqual(first["artifact_job_arguments"]["--BUILDER_FINGERPRINT"], self.app_hash)
        self.assertNotIn("--ALLOW_AWS_WRITES", first["artifact_job_arguments"])
        self.assertFalse(first["deployed"])
        self.assertFalse(first["published"])
        self.assertEqual(first["source_commit"], self.revision)
        self.assertEqual(len(first["sdk_wheels"]), 20)
        self.assertEqual(first["target"]["architecture"], "x86_64")
        with zipfile.ZipFile(a / "role5_modules.zip") as archive:
            self.assertEqual(archive.namelist(), sorted(release.application.GENERATION_MODULES))
            for name in archive.namelist():
                self.assertEqual(archive.read(name), self.source[name])
        self.assertNotIn(b"\r\n", (a / "glue_sample_generation.py").read_bytes())

    def test_dirty_staged_untracked_and_changed_head_are_rejected(self):
        for status in (b" M README.md\n", b"M  README.md\n", b"?? local.txt\n"):
            self.dirty = status
            self.reject()
        self.dirty = b""
        self.head = "c" * 40
        self.reject()
        self.head = self.revision
        self.reject(revision="HEAD")

    def test_invalid_generation_release_ids_and_dates(self):
        for name in ("../escape", "sample_preflight_20261105_07", "sample_generation_20260230_01",
                     "sample_generation_20261106_00", "sample_generation_20261106_1", "sample_generation_20261106_01/"):
            with self.subTest(name=name):
                self.reject(release_id=name)

    def test_existing_release_and_preflight_siblings_are_never_overwritten(self):
        earlier = self.releases / "sample_preflight_20261105_07"
        earlier.mkdir()
        marker = earlier / "glue_sample_preflight.py"
        marker.write_bytes(b"preserve successful preflight")
        result = self.assemble()
        current = self.releases / result["release_id"]
        before = (current / "evidence/release_manifest.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.assemble()
        self.assertEqual(marker.read_bytes(), b"preserve successful preflight")
        self.assertEqual((current / "evidence/release_manifest.json").read_bytes(), before)

    def test_sdk_tampering_and_known_hash_gate(self):
        payload = self.bundle.read_bytes()
        self.bundle.write_bytes(payload + b"tampered")
        self.reject()
        self.bundle.write_bytes(payload)
        with patch.object(release, "VERIFIED_SDK_SHA256", "0" * 64):
            self.reject()

    def test_wrong_lock_provenance_and_incomplete_inventory(self):
        text = self.fixture.lock.read_text()
        self.fixture.lock.write_text(text.replace(self.fixture.records["boto3"]["sha256"], "0" * 64), encoding="utf-8")
        self.reject()
        self.fixture.save_evidence()
        self.fixture.provenance.write_text(json.dumps(list(self.fixture.records.values())[:-1]), encoding="utf-8")
        self.reject()

    def test_report_hash_platform_versions_inventory_and_success_evidence(self):
        original = json.dumps(self.report)
        cases = [("platform", "win32"), ("installed", {}), ("pip_check", "broken requirements"),
                 ("requirements_sha256", "0" * 64), ("application_zip_sha256", "0" * 64),
                 ("zip_imports", "not verified"), ("wheels", self.report["wheels"][:-1]),
                 ("environment", {"python": "3.12.5"}), ("environment", {"python": 311}), ("model", {})]
        for field, value in cases:
            self.report = json.loads(original)
            self.report[field] = value
            self.save_report()
            with self.subTest(field=field):
                self.reject()

    def test_missing_evidence_and_false_conditional_header_claims(self):
        original = json.dumps(self.report)
        for field in ("model", "platform", "application_zip_sha256", "requirements_sha256", "wheels", "pip_version"):
            self.report = json.loads(original)
            del self.report[field]
            self.save_report()
            self.reject()
        self.report = json.loads(original)
        self.report["model"]["serialized_headers"]["If-Match"] = "wrong"
        self.save_report()
        self.reject()
        self.reject(verification=self.root / "missing.json")

    def test_missing_symlink_git_blobs_and_changed_committed_tooling(self):
        name = "glue_sample_generation.py"
        content = self.source.pop(name)
        self.reject()
        self.source[name] = content
        self.modes[name] = "120000"
        self.reject()
        self.modes[name] = "100644"
        self.source["package_glue_modules.py"] += b"# different tool\n"
        self.reject()

    def test_source_change_during_assembly_leaves_no_success_manifest(self):
        original = self.git
        calls = 0
        def changed(repo, *args):
            nonlocal calls
            if args[0] == "status":
                calls += 1
                if calls == 3:
                    return b" M changed.py\n"
            return original(repo, *args)
        with patch.object(release, "_git", side_effect=changed):
            with self.assertRaises(ValueError):
                self.assemble()
        directory = self.releases / "sample_generation_20261106_01"
        self.assertTrue(directory.exists())
        self.assertFalse((directory / "evidence/release_manifest.json").exists())
        with self.assertRaises(FileExistsError):
            self.assemble()

    def test_unsafe_output_locations_and_reparse_paths(self):
        self.reject(output_root=self.repo)
        self.reject(output_root=self.root / "missing")
        original = Path.lstat
        def lstat(path, *args, **kwargs):
            if path == self.releases:
                from types import SimpleNamespace
                return SimpleNamespace(st_mode=stat.S_IFDIR, st_file_attributes=0x400)
            return original(path, *args, **kwargs)
        with patch.object(Path, "lstat", lstat):
            self.reject()

    def test_offline_only_and_only_read_only_git_commands(self):
        with patch.object(socket.socket, "connect", side_effect=AssertionError("network")), \
             patch.dict(sys.modules, {"boto3": None, "botocore": None, "pyspark": None, "awsglue": None}):
            self.assemble()
        self.assertTrue(self.mock_git.call_args_list)
        self.assertTrue(all(call.args[1] in ("status", "rev-parse", "ls-tree", "show") for call in self.mock_git.call_args_list))

    def test_altered_sdk_structure_cannot_be_relabelled_as_verified(self):
        with zipfile.ZipFile(self.bundle, "r") as archive:
            payloads = {name: archive.read(name) for name in archive.namelist()}
        payloads.pop(next(name for name in payloads if name.endswith(".whl")))
        with zipfile.ZipFile(self.bundle, "w") as archive:
            for name, content in payloads.items():
                archive.writestr(name, content)
        # Even if a caller changed the outer trust anchor, closure checks remain.
        with patch.object(release, "VERIFIED_SDK_SHA256", release._sha(self.bundle.read_bytes())):
            self.reject()

    def test_committed_application_change_requires_fresh_import_evidence(self):
        self.source["sequence_batch.py"] += b"# committed application change\n"
        self.reject()

    def test_write_failure_preserves_incomplete_directory_without_manifest(self):
        original = Path.open
        def opening(path, *args, **kwargs):
            if path.name == "role5_modules.zip" and path.parent.parent == self.releases:
                raise OSError("injected release write failure")
            return original(path, *args, **kwargs)
        with patch.object(Path, "open", opening):
            with self.assertRaisesRegex(OSError, "injected release write failure"):
                self.assemble()
        directory = self.releases / "sample_generation_20261106_01"
        self.assertTrue(directory.exists())
        self.assertFalse((directory / "evidence/release_manifest.json").exists())
        with self.assertRaises(FileExistsError):
            self.assemble()


if __name__ == "__main__":
    unittest.main()
