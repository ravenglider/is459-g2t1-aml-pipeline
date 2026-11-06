"""C4b candidate verifier; never installs packages or calls AWS.

Workflow (external disposable Python 3.11 venv): pip download --only-binary=:all:
-r requirements_sdk_candidate.txt; `provenance` creates a PyPI-verified hash lock;
pip install --no-index --find-links WHEELS --require-hashes -r LOCK --report REPORT;
`verify` performs offline model/serialization/import checks and pip check.
Public PyPI access occurs ONLY in the explicit provenance command. No credentials
or boto3 clients are needed. Windows verification is not Linux/Glue certification.
The application ZIP stays separate; optional Spark site-packages are read-only.
"""

import argparse
from email.parser import BytesParser
import hashlib
import importlib.metadata
import json
from pathlib import Path
import re
import socket
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch
import urllib.request
import zipfile


def pins(path):
    result = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"([a-z0-9-]+)==([0-9][a-zA-Z0-9.]*)", line)
        if not match or match[1] in result:
            raise ValueError("Only unique exact package pins are accepted")
        result[match[1]] = match[2]
    if not result:
        raise ValueError("Empty requirements")
    return result


def check_disposable(workspace):
    workspace = Path(workspace).resolve()
    prefix = Path(sys.prefix).resolve()
    repo = Path(__file__).resolve().parents[2]
    if (sys.version_info[:2] != (3, 11) or sys.prefix == sys.base_prefix
            or prefix != workspace / "venv" or workspace.is_relative_to(repo)
            or repo.is_relative_to(workspace)):
        raise ValueError("Requires Python 3.11 in an external WORKSPACE/venv")
    return dict(interpreter=sys.executable, python=sys.version, prefix=str(prefix),
                disposable_workspace=str(workspace))


def wheel_records(wheelhouse, expected):
    records = []
    found = {}
    for path in sorted(Path(wheelhouse).glob("*.whl")):
        with zipfile.ZipFile(path) as archive:
            metadata = [n for n in archive.namelist() if n.endswith(".dist-info/METADATA")]
            if len(metadata) != 1:
                raise ValueError("Wheel must have exactly one METADATA")
            meta = BytesParser().parsebytes(archive.read(metadata[0]))
        name = meta["Name"].lower().replace("_", "-")
        version = meta["Version"]
        if name in found or expected.get(name) != version:
            raise ValueError("Unexpected, duplicate or unpinned wheel: " + name)
        found[name] = version
        records.append(dict(name=name, version=version, filename=path.name,
                            sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    if found != expected:
        raise ValueError("Wheelhouse does not contain the complete pinned dependency set")
    return records


def provenance(wheelhouse, requirements, output, lock):
    """Explicit PUBLIC PyPI metadata fetch; verify each wheel before locking it."""
    records = wheel_records(wheelhouse, pins(requirements))
    for record in records:
        url = f"https://pypi.org/pypi/{record['name']}/{record['version']}/json"
        with urllib.request.urlopen(url, timeout=30) as response:
            metadata = json.load(response)
        matches = [item for item in metadata["urls"] if item["filename"] == record["filename"]]
        if (len(matches) != 1 or matches[0]["digests"]["sha256"] != record["sha256"]
                or not matches[0]["url"].startswith("https://files.pythonhosted.org/")):
            raise ValueError("Wheel failed public PyPI provenance verification")
        record.update(metadata_url=url, download_url=matches[0]["url"])
    Path(output).write_text(json.dumps(records, indent=2) + "\n", encoding="utf-8")
    Path(lock).write_text("".join(f"{r['name']}=={r['version']} --hash=sha256:{r['sha256']}\n"
                                  for r in records), encoding="utf-8")
    return records


def probe_model(service):
    """Actual botocore serializer; no signing, client creation or HTTP transport."""
    from botocore.serialize import create_serializer
    from sequence_glue_runtime import validate_s3_conditional_writes
    capability = validate_s3_conditional_writes(SimpleNamespace(meta=SimpleNamespace(service_model=service)))
    operation = service.operation_model("PutObject")
    headers = {}
    for parameter, value, header in (("IfNoneMatch", "*", "If-None-Match"),
                                      ("IfMatch", '"offline-etag"', "If-Match")):
        request = create_serializer(service.metadata["protocol"]).serialize_to_request(
            dict(Bucket="offline-example", Key="not-sent", Body=b"offline", **{parameter: value}), operation)
        if request["headers"].get(header) != value:
            raise ValueError("Conditional header serialization failed: " + parameter)
        headers[header] = value
    return dict(capability=capability, serialized_headers=headers, requests_sent=0)


IMPORT_PROBE = '''
import importlib, socket, sys, zipfile
from pathlib import Path
from unittest.mock import patch
archive, spark_path = sys.argv[1:]
if spark_path:
    sys.path.append(spark_path)
def denied(*a, **k): raise AssertionError('Network/Spark/client creation forbidden')
with patch.object(socket.socket, 'connect', denied), patch.object(socket.socket, 'connect_ex', denied), patch.object(socket.socket, 'sendto', denied):
    import boto3, botocore, s3transfer, aiobotocore, s3fs, fsspec
    for module in (boto3, botocore, s3transfer, aiobotocore, s3fs, fsspec):
        assert Path(module.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()), module.__file__
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    sys.path.insert(0, archive)
    with patch.object(boto3, 'client', denied), patch.object(boto3.session.Session, '__init__', denied), patch.object(SparkContext, '__init__', denied), patch.object(SparkSession.Builder, 'getOrCreate', denied):
        names = zipfile.ZipFile(archive).namelist()
        assert len(names) == 13 and len(set(names)) == 13
        for name in names:
            module = importlib.import_module(name[:-3])
            assert module.__file__.startswith(archive), module.__file__
    assert SparkContext._active_spark_context is None
print('13 generation modules imported; no network/client/Spark startup')
'''


def verify(args):
    environment = check_disposable(args.workspace)
    expected = pins(args.requirements)
    records = wheel_records(args.wheelhouse, expected)
    saved = json.loads(Path(args.provenance).read_text(encoding="utf-8"))
    if [{k: r[k] for k in records[0]} for r in saved] != records:
        raise ValueError("Wheel inventory differs from verified provenance")
    installed = {name: importlib.metadata.version(name) for name in expected}
    if installed != expected:
        raise ValueError("Installed versions differ from complete candidate pins")
    installation = json.loads(Path(args.install_report).read_text(encoding="utf-8"))
    hashes = {r["sha256"] for r in records}
    installed_hashes = {r["download_info"]["archive_info"]["hashes"]["sha256"] for r in installation["install"]}
    if hashes != installed_hashes:
        raise ValueError("Installed wheel hashes differ from verified wheel inventory")
    check = subprocess.run([sys.executable, "-m", "pip", "check"], capture_output=True, text=True, check=True)
    from botocore.loaders import Loader
    from botocore.model import ServiceModel
    with patch.object(socket.socket, "connect", side_effect=AssertionError("Network forbidden")), \
         patch.object(socket.socket, "connect_ex", side_effect=AssertionError("Network forbidden")), \
         patch.object(socket.socket, "sendto", side_effect=AssertionError("Network forbidden")):
        model = probe_model(ServiceModel(Loader().load_service_model("s3", "service-2"), service_name="s3"))
    from package_glue_modules import GENERATION_MODULES
    with zipfile.ZipFile(args.application_zip) as archive:
        if (len(archive.namelist()) != len(GENERATION_MODULES)
                or set(archive.namelist()) != set(GENERATION_MODULES) or archive.testzip() is not None):
            raise ValueError("Invalid generation application ZIP")
    imported = subprocess.run([sys.executable, "-I", "-c", IMPORT_PROBE,
                               str(Path(args.application_zip).resolve()), args.spark_site_packages or ""],
                              cwd=args.workspace, capture_output=True, text=True, check=True)
    result = dict(environment=environment, installed=installed, wheels=saved,
                  pip_check=check.stdout.strip(), model=model, zip_imports=imported.stdout.strip(),
                  pip_version=importlib.metadata.version("pip"),
                  requirements_sha256=hashlib.sha256(Path(args.requirements).read_bytes()).hexdigest(),
                  application_zip_sha256=hashlib.sha256(Path(args.application_zip).read_bytes()).hexdigest(),
                  platform=sys.platform, limitation="Not a managed Glue or Linux native-wheel certification")
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    public = commands.add_parser("provenance")
    offline = commands.add_parser("verify")
    for command in (public, offline):
        command.add_argument("--wheelhouse", required=True)
        command.add_argument("--requirements", required=True)
        command.add_argument("--output", required=True)
    public.add_argument("--lock", required=True)
    for name in ("workspace", "provenance", "install-report", "application-zip"):
        offline.add_argument("--" + name, required=True)
    offline.add_argument("--spark-site-packages")
    args = parser.parse_args()
    if args.command == "provenance":
        provenance(args.wheelhouse, args.requirements, args.output, args.lock)
    else:
        verify(args)
    print("VERIFIED: " + args.command)


if __name__ == "__main__":
    main()
