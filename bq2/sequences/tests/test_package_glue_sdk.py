"""Synthetic wheel/evidence tests only; no downloaded Linux inventory or AWS."""

import hashlib
import io
import json
from pathlib import Path
import socket
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import package_glue_sdk as sdk


class SDKBundleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.wheels = self.root / "wheels"
        self.wheels.mkdir()
        self.lock = self.root / "linux.lock.txt"
        self.provenance = self.root / "provenance.json"
        self.output = self.root / "role5_sdk.gluewheels.zip"
        self.approved = sdk.pins(sdk.REQUIREMENTS)
        self.records = {}
        for name in self.approved:
            self.replace(name)

    def save_evidence(self):
        records = list(self.records.values())
        self.lock.write_text("".join(f"{r['name']}=={r['version']} --hash=sha256:{r['sha256']}\n"
                                      for r in records), encoding="utf-8")
        self.provenance.write_text(json.dumps(records), encoding="utf-8")

    def replace(self, name="aiohttp", tag="py3-none-any", *, metadata=None, wheel=None, extras=()):
        if name in self.records:
            (self.wheels / self.records[name]["filename"]).unlink()
        version = self.approved[name]
        distribution = name.replace("-", "_")
        filename = f"{distribution}-{version}-{tag}.whl"
        prefix = f"{distribution}-{version}.dist-info/"
        meta = (f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\nRequires-Python: >=3.8\n"
                if metadata is None else metadata)
        if wheel is None:
            pure = "true" if all(t[2] == "any" for t in sdk._tags(tag)) else "false"
            wheel = "Wheel-Version: 1.0\nRoot-Is-Purelib: " + pure + "\n"
            wheel += "".join("Tag: " + "-".join(t) + "\n" for t in sorted(sdk._tags(tag)))
        with zipfile.ZipFile(self.wheels / filename, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(prefix + "METADATA", meta)
            archive.writestr(prefix + "WHEEL", wheel)
            archive.writestr(prefix + "RECORD", "")
            archive.writestr(distribution + "/__init__.py", "# synthetic fixture, not the actual SDK\n")
            for path, content in extras:
                archive.writestr(path, content)
        # Windows ZipInfo normalizes backslashes when writing; inject the raw
        # malformed filename into both ZIP headers so this is a real bad fixture.
        payload = (self.wheels / filename).read_bytes()
        for path, _ in extras:
            if isinstance(path, str) and "\\" in path:
                payload = payload.replace(path.replace("\\", "/").encode(), path.encode())
        (self.wheels / filename).write_bytes(payload)
        self.records[name] = dict(name=name, version=version, filename=filename,
            sha256=hashlib.sha256((self.wheels / filename).read_bytes()).hexdigest(),
            metadata_url=f"https://pypi.org/pypi/{name}/{version}/json",
            download_url="https://files.pythonhosted.org/packages/synthetic/" + filename)
        self.save_evidence()

    def build(self, output=None):
        return sdk.build_sdk_bundle(output or self.output, wheelhouse=self.wheels,
                                    lock=self.lock, provenance=self.provenance)

    def reject(self):
        with self.assertRaises((ValueError, zipfile.BadZipFile)):
            self.build()
        self.assertFalse(self.output.exists())

    def test_layout_bytes_metadata_and_two_fresh_builds_are_identical(self):
        first, second = self.root / "first", self.root / "second"
        first.mkdir()
        second.mkdir()
        a, b = first / self.output.name, second / self.output.name
        self.assertEqual(self.build(a), self.build(b))
        self.assertEqual(a.read_bytes(), b.read_bytes())
        with zipfile.ZipFile(a) as archive:
            expected = {"wheels/" + r["filename"] for r in self.records.values()} | {"wheels/requirements.txt"}
            self.assertEqual(archive.namelist(), sorted(expected))
            self.assertEqual(len(archive.infolist()), 21)
            self.assertIsNone(archive.testzip())
            expected_pins = "".join(f"{name}=={self.approved[name]}\n" for name in sorted(self.approved))
            self.assertEqual(archive.read("wheels/requirements.txt").decode(), expected_pins)
            for info in archive.infolist():
                self.assertEqual(info.date_time, (1980, 1, 1, 0, 0, 0))
                self.assertEqual(info.create_system, 3)
                self.assertEqual(info.external_attr >> 16, 0o100644)
                self.assertEqual(info.compress_type, zipfile.ZIP_STORED)
            for row in self.records.values():
                self.assertEqual(archive.read("wheels/" + row["filename"]), (self.wheels / row["filename"]).read_bytes())

    def test_missing_duplicate_and_extra_dependency_closure(self):
        row = self.records["boto3"]
        path = self.wheels / row["filename"]
        content = path.read_bytes()
        path.unlink()
        self.reject()
        path.write_bytes(content)
        extra = self.wheels / "unexpected-1.0-py3-none-any.whl"
        extra.write_bytes(content)
        self.reject()
        extra.unlink()
        rows = list(self.records.values())
        for records in (rows[:-1], rows + [rows[0]], rows[:-1] + [rows[0]]):
            self.provenance.write_text(json.dumps(records), encoding="utf-8")
            self.reject()

    def test_source_distributions_hidden_files_and_directories_rejected(self):
        for name in ("package.tar.gz", ".DS_Store", "requirements.txt", "secret.env", "test.py"):
            with self.subTest(name=name):
                path = self.wheels / name
                path.write_text("unexpected", encoding="utf-8")
                self.reject()
                path.unlink()
        extra = self.wheels / "nested"
        extra.mkdir()
        self.reject()

    def test_wheel_hash_and_frozen_lock_mismatches(self):
        path = self.wheels / self.records["boto3"]["filename"]
        content = path.read_bytes()
        path.write_bytes(content + b"changed")
        self.reject()
        path.write_bytes(content)
        text = self.lock.read_text()
        self.lock.write_text(text.replace(self.records["boto3"]["sha256"], "0" * 64), encoding="utf-8")
        self.reject()

    def test_lock_must_be_exact_complete_unique_and_hashed(self):
        original = self.lock.read_text()
        for text in (original.splitlines()[0], original + original.splitlines()[0],
                     original.replace("boto3==1.35.93", "boto3==1.35.94"),
                     original.replace("--hash=sha256:", "--hash=md5:"),
                     original + "--no-deps\n", original.replace("boto3==", "boto3>=")):
            with self.subTest(text=text[:70]):
                self.lock.write_text(text, encoding="utf-8")
                self.reject()

    def test_provenance_identity_urls_and_duplicate_json_keys(self):
        original = list(self.records.values())
        for field, value in (("filename", "../bad.whl"), ("version", "99.0"), ("name", "unrelated"),
                             ("metadata_url", "https://example.test/metadata"),
                             ("download_url", "https://files.pythonhosted.org.evil.test/package.whl"),
                             ("download_url", original[0]["download_url"] + "?token=value"),
                             ("download_url", original[0]["download_url"] + "#fragment"),
                             ("download_url", "https://files.pythonhosted.org/packages/wrong.whl")):
            rows = [dict(row) for row in original]
            rows[0][field] = value
            self.provenance.write_text(json.dumps(rows), encoding="utf-8")
            with self.subTest(field=field, value=value):
                self.reject()
        self.provenance.write_text(json.dumps(original).replace('"name":', '"name": "duplicate", "name":', 1), encoding="utf-8")
        self.reject()

    def test_compatible_python_and_older_manylinux_tags(self):
        tags = ("py2.py3-none-any", "py311-none-any", "cp311-cp311-manylinux1_x86_64",
                "cp311-cp311-manylinux2010_x86_64", "cp311-cp311-manylinux_2_5_x86_64.manylinux1_x86_64",
                "cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64", "cp38-abi3-manylinux2014_x86_64")
        for index, tag in enumerate(tags):
            self.replace(tag=tag)
            with self.subTest(tag=tag):
                self.build(self.root / f"accepted_{index}.gluewheels.zip")

    def test_incompatible_platforms_abis_and_python_versions(self):
        tags = ("cp311-cp311-win_amd64", "cp311-cp311-macosx_10_9_x86_64", "cp311-cp311-manylinux2014_aarch64",
                "cp311-cp311-musllinux_1_2_x86_64", "cp311-cp311-linux_x86_64", "cp311-cp311-manylinux_2_28_x86_64",
                "cp310-cp310-manylinux2014_x86_64", "cp312-cp312-manylinux2014_x86_64", "cp312-abi3-manylinux2014_x86_64",
                "cp311-cp310-manylinux2014_x86_64", "cp311-cp311-any", "py2-none-any",
                "py3-none-any.win_amd64", "cp311-cp311-manylinux2014_i686")
        for tag in tags:
            self.replace(tag=tag)
            with self.subTest(tag=tag):
                self.reject()

    def test_metadata_name_version_python_and_wheel_declarations(self):
        base = f"Metadata-Version: 2.1\nName: aiohttp\nVersion: {self.approved['aiohttp']}\n"
        bad = (base.replace("aiohttp", "unrelated"), base.replace("3.10.1", "3.10.2"),
               base + "Name: aiohttp\n", base + "Requires-Python: >=3.12\n",
               base + "Requires-Python: >=3.8\nRequires-Python: >=3.8\n",
               base + "Requires-Python: nonsense\n")
        for metadata in bad:
            self.replace(metadata=metadata)
            self.reject()
        for wheel in ("Wheel-Version: 2.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
                      "Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: py3-none-any\n",
                      "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py2-none-any\n",
                      "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\nTag: py3-none-any\n"):
            self.replace(wheel=wheel)
            self.reject()

    def test_internal_unsafe_duplicate_and_symlink_entries(self):
        for name in ("../escape", "/absolute", "a/../../escape", "a\\escape", "C:/escape", "a//escape", "a/./escape", "a//"):
            self.replace(extras=[(name, "bad")])
            with self.subTest(name=name):
                self.reject()
        link = zipfile.ZipInfo("linked")
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        self.replace(extras=[(link, "../escape")])
        self.reject()
        with self.assertWarns(UserWarning):
            self.replace(extras=[("aiohttp/__init__.py", "duplicate")])
        self.reject()
        self.replace(extras=[("conflict", "file"), ("conflict/child.py", "# invalid parent")])
        self.reject()

    def test_corrupt_missing_and_foreign_wheel_metadata(self):
        self.replace(extras=[("other-1.0.dist-info/METADATA", "foreign")])
        self.reject()
        self.replace()
        path = self.wheels / self.records["aiohttp"]["filename"]
        for content in (b"not a ZIP", self.missing_metadata_wheel()):
            path.write_bytes(content)
            self.records["aiohttp"]["sha256"] = hashlib.sha256(content).hexdigest()
            self.save_evidence()
            self.reject()

    def missing_metadata_wheel(self):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            archive.writestr("aiohttp/__init__.py", "# missing metadata")
        return output.getvalue()

    def test_filename_identity_and_tag_malformation(self):
        original = self.records["aiohttp"]["filename"]
        content = (self.wheels / original).read_bytes()
        for filename in (original.replace("aiohttp", "wrong"), original.replace("3.10.1", "3.10.2"),
                         "aiohttp-3.10.1-bad-py3-none-any.whl", "aiohttp-3.10.1-py3-none-any-any.whl"):
            self.records["aiohttp"]["filename"] = filename
            self.records["aiohttp"]["download_url"] = "https://files.pythonhosted.org/packages/" + filename
            (self.wheels / original).unlink()
            (self.wheels / filename).write_bytes(content)
            self.save_evidence()
            self.reject()
            (self.wheels / filename).unlink()
            (self.wheels / original).write_bytes(content)

    def test_existing_output_and_unsafe_destination_paths(self):
        self.output.write_bytes(b"previous artifact")
        with self.assertRaises(FileExistsError):
            self.build()
        self.assertEqual(self.output.read_bytes(), b"previous artifact")
        for path in (self.root / "missing" / self.output.name, self.wheels / self.output.name,
                     self.root / "bad.zip", self.root / "child" / ".." / "new.gluewheels.zip"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.build(path)

    def test_symlink_or_windows_reparse_input_and_output_parents_rejected(self):
        original = Path.lstat
        for selected, attributes, mode in ((self.wheels, 0, stat.S_IFLNK), (self.root, 0x400, stat.S_IFDIR),
                                            (self.output, 0, stat.S_IFLNK),
                                            (self.wheels / self.records["boto3"]["filename"], 0, stat.S_IFLNK)):
            def lstat(path, *args, **kwargs):
                return SimpleNamespace(st_mode=mode, st_file_attributes=attributes) if path == selected else original(path, *args, **kwargs)
            with self.subTest(path=selected), patch.object(Path, "lstat", lstat):
                self.reject()

    def test_no_network_process_installation_or_application_zip(self):
        with patch.object(socket.socket, "connect", side_effect=AssertionError("network")), \
             patch.object(subprocess, "run", side_effect=AssertionError("installation/process")), \
             patch.dict(sys.modules, {"boto3": None, "botocore": None, "pyspark": None}):
            self.build()
        with zipfile.ZipFile(self.output) as archive:
            self.assertFalse(any(name.endswith(".py") or "role5_modules.zip" in name for name in archive.namelist()))

    def test_current_style_python_constraints_and_unknown_syntax_fail_closed(self):
        for value in (">=3.8", ">=2.7, !=3.0.*, !=3.1.*, !=3.2.*", ">=3.8,<4", "==3.*", "==3.11.*"):
            sdk._python_requirement(value)
        for value in (">=3.12", "<3.11", "!=3.11.*", "==2.*", ">=3.11.1", "~=3.8", "unknown"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                sdk._python_requirement(value)

    def test_interrupted_write_is_preserved_and_never_overwritten(self):
        with patch.object(zipfile.ZipFile, "writestr", side_effect=OSError("injected write failure")):
            with self.assertRaisesRegex(OSError, "injected write failure"):
                self.build()
        self.assertTrue(self.output.exists())
        partial = self.output.read_bytes()
        with self.assertRaises(FileExistsError):
            self.build()
        self.assertEqual(self.output.read_bytes(), partial)
        self.build(self.root / "retry.gluewheels.zip")

    def test_cli_works_in_isolated_interpreter(self):
        # -I excludes even the script directory: explicitly expose only the trusted
        # tooling directory for its existing verifier helper, never ambient cwd.
        launcher = "import runpy,sys; from pathlib import Path; sys.path.insert(0,str(Path(sys.argv[1]).parent)); sys.argv=sys.argv[1:]; runpy.run_path(sys.argv[0],run_name='__main__')"
        result = subprocess.run([sys.executable, "-I", "-c", launcher, str(ROOT / "package_glue_sdk.py"),
            "--wheelhouse", str(self.wheels), "--lock", str(self.lock), "--provenance", str(self.provenance),
            "--output", str(self.output)], cwd=self.root, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), hashlib.sha256(self.output.read_bytes()).hexdigest())


if __name__ == "__main__":
    unittest.main()
