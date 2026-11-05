"""Assemble a fresh offline generation release from clean HEAD and C4b/C5a evidence.

No installation, network, Git writes, AWS clients or deployment. Existing SDK
evidence is a trusted attestation, not a signature or managed-Glue certification.
The confirmed C5a bundle hash is pinned deliberately; another bundle needs review.
Parents must remain stable during assembly. Interrupted directories are preserved;
the release manifest is written last, and an existing release is never reused.
"""

import argparse
from datetime import datetime
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import subprocess
import tempfile
import zipfile

import package_glue_modules as application
import package_glue_sdk as sdk
import verify_glue_sdk as verifier


VERIFIED_SDK_SHA256 = "86eebac430c9a9983ee1626f6ef87890b781589236ac68f6b1dcbc5e42951ce0"
PREFIX = "bq2/sequences/"
SCRIPT_ROOT = "s3://is459-g2t1-aml-athena-bucket/scripts/role5/"
TARGET = dict(glue="5.0", python="3.11", architecture="x86_64", os="linux",
              wheel_policy="manylinux-glibc-2.17-or-older")


def _sha(payload):
    return hashlib.sha256(payload).hexdigest()


def _lf(payload):
    return payload.decode("utf-8").replace("\r\n", "\n").encode("utf-8")


def _git(repo, *arguments):
    return subprocess.run(["git", "--no-optional-locks", "-C", str(repo), *arguments],
                          check=True, capture_output=True).stdout


def _clean_head(repo, revision):
    if _git(repo, "status", "--porcelain=v1", "--untracked-files=all").strip():
        raise ValueError("Release source must be completely clean, including untracked files")
    if _git(repo, "rev-parse", "--verify", "HEAD^{commit}").decode().strip() != revision:
        raise ValueError("Requested source commit must be the clean checked-out HEAD")


def _source(repo, revision):
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise ValueError("Explicit full committed Git revision required")
    top = Path(_git(repo, "rev-parse", "--show-toplevel").decode().strip()).resolve()
    if top != repo.resolve():
        raise ValueError("Supply the Git repository root")
    _clean_head(repo, revision)
    modules = application.GENERATION_MODULES
    if len(modules) != 13 or len(set(modules)) != 13 or any(not re.fullmatch(r"[a-z0-9_]+\.py", n) for n in modules):
        raise ValueError("Expected the approved 13-module generation profile")
    names = set(modules) | {"glue_sample_generation.py", "package_glue_modules.py", "package_glue_sdk.py",
                            "verify_glue_sdk.py", "requirements_sdk_candidate.txt"}
    entries = _git(repo, "ls-tree", "-r", "-z", revision, "--", *[PREFIX + n for n in sorted(names)])
    modes = {}
    for entry in entries.split(b"\0"):
        if entry:
            header, path = entry.split(b"\t", 1)
            mode, kind, _ = header.split()
            if kind != b"blob" or mode not in (b"100644", b"100755"):
                raise ValueError("Committed release inputs must be regular Git blobs")
            modes[path.decode()] = mode
    if set(modes) != {PREFIX + n for n in names}:
        raise ValueError("Committed source is missing required release inputs")
    source = {name: _lf(_git(repo, "show", revision + ":" + PREFIX + name)) for name in names}
    # Reused validators must be the same tooling as the committed source, rather
    # than a different checkout's interpretation of the frozen evidence.
    for module in (application, sdk, verifier):
        path = sdk._safe_path(module.__file__, "file")
        if _lf(path.read_bytes()) != source[path.name]:
            raise ValueError("Assembler tooling differs from selected committed source")
    if _lf(sdk._safe_path(sdk.REQUIREMENTS, "file").read_bytes()) != source["requirements_sdk_candidate.txt"]:
        raise ValueError("Candidate requirements differ from committed source")
    return source


def _verification(raw, inventory, approved, requirements_hash, application_hash):
    report = json.loads(raw, object_pairs_hook=sdk._unique_object)
    fields = {"environment", "installed", "wheels", "pip_check", "model", "zip_imports", "pip_version",
              "requirements_sha256", "application_zip_sha256", "platform", "limitation"}
    if not isinstance(report, dict) or set(report) != fields:
        raise ValueError("Expected a C4b verification object")
    environment = report.get("environment", {})
    model = report.get("model", {})
    capability = model.get("capability", {}) if isinstance(model, dict) else {}
    if (not isinstance(environment, dict)
            or set(environment) != {"interpreter", "python", "prefix", "disposable_workspace"}
            or any(not isinstance(v, str) or not v.strip() for v in environment.values())
            or report.get("platform") != "linux"
            or not re.match(r"3\.11\.[0-9]+(?:\s|$)", environment["python"])
            or not isinstance(report["pip_version"], str) or not report["pip_version"]
            or not isinstance(report["limitation"], str) or not report["limitation"]
            or report.get("installed") != approved or report.get("pip_check") != "No broken requirements found."
            or report.get("requirements_sha256") != requirements_hash
            or report.get("application_zip_sha256") != application_hash
            or report.get("zip_imports") != "13 generation modules imported; no network/client/Spark startup"
            or not isinstance(model, dict) or set(model) != {"capability", "serialized_headers", "requests_sent"}
            or not isinstance(capability, dict)
            or set(capability) != {"status", "operation", "conditional_writes_supported", "required_capabilities", "missing_capabilities"}
            or capability.get("status") != "COMPATIBLE" or capability.get("operation") != "PutObject"
            or capability.get("conditional_writes_supported") is not True
            or capability.get("required_capabilities") != ["IfNoneMatch", "IfMatch"]
            or capability.get("missing_capabilities") != []
            or type(model.get("requests_sent")) is not int or model.get("requests_sent") != 0
            or model.get("serialized_headers") != {"If-None-Match": "*", "If-Match": '"offline-etag"'}):
        raise ValueError("Missing, unsupported or mismatched Linux SDK verification evidence")
    wheels = report.get("wheels")
    expected = sorted(inventory.values(), key=lambda r: r["filename"])
    if (not isinstance(wheels, list) or len(wheels) != 20
            or any(not isinstance(r, dict) or set(r) != set(expected[0])
                   or any(not isinstance(v, str) for v in r.values()) for r in wheels)
            or sorted(wheels, key=lambda r: r.get("filename", "")) != expected):
        raise ValueError("Verification wheel inventory differs from frozen provenance")


def assemble_release(output_root, release_id, *, repo, revision, sdk_bundle, lock, provenance, verification):
    """Create release_id exclusively; return the deterministic offline manifest.

    Verification evidence must bind the newly built application ZIP. No override
    permits unverified bundles, dirty source or unsupported targets.
    """
    match = re.fullmatch(r"sample_generation_([0-9]{8})_([0-9]{2})", release_id)
    if not match or match[2] == "00":
        raise ValueError("Use sample_generation_YYYYMMDD_NN with sequence 01-99")
    datetime.strptime(match[1], "%Y%m%d")
    repo = sdk._safe_path(repo, "directory")
    root = sdk._safe_path(output_root, "directory")
    if root.is_relative_to(repo):
        raise ValueError("Release root must be outside the source repository")
    destination = sdk._safe_path(root / release_id, "new")
    source = _source(repo, revision)
    paths = {name: sdk._safe_path(path, "file") for name, path in (
        ("role5_sdk.gluewheels.zip", sdk_bundle), ("sdk-linux.lock.txt", lock),
        ("sdk-provenance.json", provenance), ("sdk-verification.json", verification))}
    inputs = {name: path.read_bytes() for name, path in paths.items()}
    sdk_bytes = inputs["role5_sdk.gluewheels.zip"]
    if _sha(sdk_bytes) != VERIFIED_SDK_SHA256:
        raise ValueError("SDK bundle does not match the confirmed C5a SHA-256")
    with tempfile.TemporaryDirectory(prefix="role5-release-check-") as folder:
        temporary = Path(folder)
        snapshot = temporary / "source"
        snapshot.mkdir()
        for name, payload in source.items():
            (snapshot / name).write_bytes(payload)
        spec = importlib.util.spec_from_file_location("_committed_role5_packager", snapshot / "package_glue_modules.py")
        packager = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(packager)
        if packager.GENERATION_MODULES != application.GENERATION_MODULES:
            raise ValueError("Committed generation profile mismatch")
        app_path = temporary / "role5_modules.zip"
        application_hash = packager.build_package(app_path, profile="generation")
        app_bytes = app_path.read_bytes()
        if _sha(app_bytes) != application_hash:
            raise ValueError("Application builder fingerprint mismatch")
        local_lock, local_provenance = temporary / "sdk.lock.txt", temporary / "provenance.json"
        local_lock.write_bytes(inputs["sdk-linux.lock.txt"])
        local_provenance.write_bytes(inputs["sdk-provenance.json"])
        approved = verifier.pins(snapshot / "requirements_sdk_candidate.txt")
        inventory = sdk._frozen_inventory(local_lock, local_provenance, approved)
        wheels = temporary / "wheels"
        wheels.mkdir()
        with zipfile.ZipFile(io.BytesIO(sdk_bytes)) as archive:
            names = archive.namelist()
            expected = {"wheels/requirements.txt"} | {"wheels/" + n for n in inventory}
            if len(names) != 21 or set(names) != expected or archive.testzip() is not None:
                raise ValueError("SDK bundle must contain exactly the frozen wheel inventory and requirements")
            # Never extractall: only allowlisted, flat wheel names are copied.
            for name in inventory:
                (wheels / name).write_bytes(archive.read("wheels/" + name))
        rebuilt = temporary / "rebuilt.gluewheels.zip"
        if sdk.build_sdk_bundle(rebuilt, wheelhouse=wheels, lock=local_lock, provenance=local_provenance) != VERIFIED_SDK_SHA256:
            raise ValueError("SDK bundle differs from deterministic C5a reconstruction")
        requirements_hash = _sha(source["requirements_sdk_candidate.txt"])
        _verification(inputs["sdk-verification.json"], inventory, approved, requirements_hash, application_hash)
    artifacts = {"glue_sample_generation.py": source["glue_sample_generation.py"],
                 "role5_modules.zip": app_bytes, "role5_sdk.gluewheels.zip": sdk_bytes}
    evidence = {"evidence/" + name: inputs[name] for name in (
        "sdk-linux.lock.txt", "sdk-provenance.json", "sdk-verification.json")}
    artifact_info = {name: dict(filename=name, sha256=_sha(payload), size_bytes=len(payload))
                     for name, payload in sorted(artifacts.items())}
    manifest = dict(format_version=1, kind="bounded_sample_generation_release", status="ASSEMBLED_OFFLINE",
        release_id=release_id, source_commit=revision, builder_fingerprint=application_hash,
        target=dict(TARGET), requirements_sha256=requirements_hash, artifacts=artifact_info,
        application_modules=sorted(application.GENERATION_MODULES),
        sdk_wheels=sorted(inventory.values(), key=lambda r: r["filename"]),
        evidence={name: dict(sha256=_sha(payload), size_bytes=len(payload)) for name, payload in sorted(evidence.items())},
        artifact_job_arguments={"--BUILDER_VERSION": revision, "--BUILDER_FINGERPRINT": application_hash,
            "--scriptLocation": SCRIPT_ROOT + release_id + "/glue_sample_generation.py",
            "--extra-py-files": SCRIPT_ROOT + release_id + "/role5_modules.zip",
            "--additional-python-modules": SCRIPT_ROOT + release_id + "/role5_sdk.gluewheels.zip",
            "--python-modules-installer-option": "--no-index"},
        deployed=False, published=False, aws_execution_verified=False,
        limitation="Frozen offline evidence only; not a managed Glue execution or permission check")
    _clean_head(repo, revision)
    destination.mkdir()  # Exclusive: no exist_ok, takeover or deletion.
    (destination / "evidence").mkdir()
    for name, payload in sorted({**artifacts, **evidence}.items()):
        path = destination / name
        with path.open("xb") as output:
            output.write(payload)
        if path.read_bytes() != payload:
            raise ValueError("Release artifact read-back mismatch")
    _clean_head(repo, revision)
    raw_manifest = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode("utf-8")
    with (destination / "evidence" / "release_manifest.json").open("xb") as output:
        output.write(raw_manifest)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    for name in ("output-root", "release-id", "repo", "revision", "sdk-bundle", "lock", "provenance", "verification"):
        parser.add_argument("--" + name, required=True)
    args = vars(parser.parse_args())
    manifest = assemble_release(**args)
    print(json.dumps(dict(status=manifest["status"], release_id=manifest["release_id"],
                          builder_fingerprint=manifest["builder_fingerprint"]), sort_keys=True))
