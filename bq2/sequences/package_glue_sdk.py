"""Offline, deterministic Glue 5.0 SDK packaging from frozen C4b evidence.

Inputs: a wheel-only directory, C4b's single-hash-per-package lock, and its
provenance JSON list. No downloads, installation, AWS calls or application ZIPs.
Evidence is trusted input: matching saved PyPI URLs is not fresh authentication.
Only the conservative x86-64 manylinux <= glibc 2.17 policy is accepted here.
Use a fresh destination outside the wheelhouse; parents must already exist and
must not be changed concurrently. Failed output writes are never retried/deleted.
"""

import argparse
from email.parser import BytesParser
import hashlib
import io
import json
from pathlib import Path
import re
import stat
from urllib.parse import urlsplit
import zipfile

from verify_glue_sdk import pins


REQUIREMENTS = Path(__file__).with_name("requirements_sdk_candidate.txt")


def _safe_path(value, kind):
    path = Path(value)
    if ".." in path.parts:
        raise ValueError("Parent traversal is not allowed")
    path = path.absolute()
    for component in (path, *path.parents):
        try:
            info = component.lstat()
        except FileNotFoundError:
            if component == path and kind == "new":
                continue
            raise ValueError("Path or parent does not exist") from None
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("Symlink/reparse paths are not allowed")
    if kind == "new":
        if path.exists():
            raise FileExistsError(path)
    elif not (path.is_dir() if kind == "directory" else path.is_file()):
        raise ValueError("Unexpected input path type")
    return path


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _frozen_inventory(lock, provenance, approved):
    locked = {}
    for line in lock.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        match = re.fullmatch(r"([a-z0-9-]+)==([0-9][A-Za-z0-9.]*) --hash=sha256:([a-f0-9]{64})", line.strip())
        if not match or match[1] in locked or approved.get(match[1]) != match[2]:
            raise ValueError("Lock must contain unique approved exact pins and one SHA-256 each")
        locked[match[1]] = match[3]
    if set(locked) != set(approved):
        raise ValueError("Lock must contain exactly the approved 20 packages")
    records = json.loads(provenance.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    if not isinstance(records, list) or len(records) != 20:
        raise ValueError("Expected the complete C4b provenance list")
    inventory = {}
    names = set()
    fields = {"name", "version", "filename", "sha256", "metadata_url", "download_url"}
    for record in records:
        if not isinstance(record, dict) or set(record) != fields or any(not isinstance(v, str) for v in record.values()):
            raise ValueError("Malformed provenance record")
        name, filename = record["name"], record["filename"]
        if (name in names or filename in inventory or name not in approved
                or approved[name] != record["version"] or locked[name] != record["sha256"]
                or not re.fullmatch(r"[A-Za-z0-9_.+-]+\.whl", filename)):
            raise ValueError("Duplicate, unsafe or mismatched frozen wheel identity")
        url = urlsplit(record["download_url"])
        if (record["metadata_url"] != f"https://pypi.org/pypi/{name}/{approved[name]}/json"
                or url.scheme != "https" or url.netloc != "files.pythonhosted.org"
                or any(c.isspace() for c in record["download_url"])
                or url.query or url.fragment or url.path.rsplit("/", 1)[-1] != filename):
            raise ValueError("Unexpected saved PyPI provenance URL")
        names.add(name)
        inventory[filename] = record
    if names != set(approved):
        raise ValueError("Incomplete provenance closure")
    return inventory


def _tags(text):
    pieces = text.split("-")
    if len(pieces) != 3 or any(not re.fullmatch(r"[a-z0-9_]+(?:\.[a-z0-9_]+)*", p) for p in pieces):
        raise ValueError("Malformed wheel tag")
    if any(len(p.split(".")) != len(set(p.split("."))) for p in pieces):
        raise ValueError("Duplicate compressed wheel tag")
    return {(python, abi, platform) for python in pieces[0].split(".")
            for abi in pieces[1].split(".") for platform in pieces[2].split(".")}


def _compatible(tags):
    usable = False
    for python, abi, platform in tags:
        if platform == "any":
            if abi != "none" or python not in ("py2", "py3", "py311"):
                raise ValueError("Unsupported universal wheel tag")
            usable |= python in ("py3", "py311")
            continue
        manylinux = re.fullmatch(r"manylinux_2_([0-9]+)_x86_64", platform)
        if (platform not in ("manylinux1_x86_64", "manylinux2010_x86_64", "manylinux2014_x86_64")
                and not (manylinux and 5 <= int(manylinux[1]) <= 17)):
            raise ValueError("Unsupported platform; require conservative manylinux x86-64")
        stable = re.fullmatch(r"cp3([0-9]{1,2})", python)
        if not ((python == "cp311" and abi == "cp311")
                or (abi == "abi3" and stable and 2 <= int(stable[1]) <= 11)
                or (python in ("py3", "py311") and abi == "none")):
            raise ValueError("Native wheel ABI/interpreter is incompatible with Python 3.11")
        usable = True
    if not usable:
        raise ValueError("Wheel has no Python 3.11 compatible tag")


def _one(message, name):
    values = message.get_all(name, [])
    if len(values) != 1:
        raise ValueError("Missing/duplicate wheel metadata field: " + name)
    return values[0]


def _python_requirement(value):
    # Deliberately narrow: current pins use major/minor bounds, not patch-specific
    # or prerelease constraints. Unknown syntax requires review, not guessing.
    for clause in value.split(","):
        match = re.fullmatch(r"\s*(>=|<=|==|!=|>|<)\s*(\d+)(?:\.(\d+))?(\.\*)?\s*", clause)
        if not match:
            raise ValueError("Unsupported Requires-Python constraint")
        op, major, minor, wildcard = match.groups()
        target, bound = (3, 11), (int(major), int(minor or 0))
        if wildcard:
            if op not in ("==", "!="):
                raise ValueError("Unsupported Python wildcard")
            equal = target[:2 if minor is not None else 1] == bound[:2 if minor is not None else 1]
            passed = equal if op == "==" else not equal
        else:
            passed = {">=": target >= bound, "<=": target <= bound, "==": target == bound,
                      "!=": target != bound, ">": target > bound, "<": target < bound}[op]
        if not passed:
            raise ValueError("METADATA excludes Python 3.11")


def _validate_wheel(filename, payload, record):
    parts = filename[:-4].split("-")
    if len(parts) not in (5, 6) or (len(parts) == 6 and not re.fullmatch(r"[0-9][A-Za-z0-9_]*", parts[2])):
        raise ValueError("Malformed wheel filename")
    distribution, version = parts[:2]
    if (not re.fullmatch(r"[A-Za-z0-9_]+", distribution)
            or distribution.lower().replace("_", "-") != record["name"] or version != record["version"]):
        raise ValueError("Wheel filename disagrees with frozen identity")
    filename_tags = _tags("-".join(parts[-3:]))
    _compatible(filename_tags)
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = set()
        nodes = set()
        for info in archive.infolist():
            path = info.filename
            node = path[:-1] if path.endswith("/") else path
            segments = node.split("/")
            mode = stat.S_IFMT(info.external_attr >> 16)
            if (info.orig_filename != path or "\\" in path or ":" in path
                    or any(ord(c) < 32 or ord(c) == 127 for c in path)
                    or any(p in ("", ".", "..") for p in segments) or node in nodes
                    or mode not in (0, stat.S_IFREG, stat.S_IFDIR) or info.flag_bits & 1
                    or (mode == stat.S_IFDIR and not path.endswith("/"))
                    or (mode == stat.S_IFREG and path.endswith("/"))
                    or info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)):
                raise ValueError("Unsafe, duplicate or malformed wheel archive entry")
            names.add(path)
            nodes.add(node)
        files = {name for name in names if not name.endswith("/")}
        if any("/".join(name.split("/")[:i]) in files
               for name in names for i in range(1, len(name.split("/")))):
            raise ValueError("Wheel file conflicts with an archive directory")
        distinfo = distribution + "-" + version + ".dist-info/"
        info_roots = {name.split("/", 1)[0] for name in names if name.split("/", 1)[0].endswith(".dist-info")}
        if (info_roots != {distinfo[:-1]}
                or not {distinfo + name for name in ("METADATA", "WHEEL", "RECORD")} <= names
                or archive.testzip() is not None):
            raise ValueError("Missing, conflicting or corrupt wheel metadata")
        metadata = BytesParser().parsebytes(archive.read(distinfo + "METADATA"))
        wheel = BytesParser().parsebytes(archive.read(distinfo + "WHEEL"))
        if (re.sub(r"[-_.]+", "-", _one(metadata, "Name")).lower() != record["name"]
                or _one(metadata, "Version") != version or not re.fullmatch(r"2\.[0-9]+", _one(metadata, "Metadata-Version"))
                or _one(wheel, "Wheel-Version") != "1.0"):
            raise ValueError("Internal wheel metadata disagrees with frozen identity/version")
        python = metadata.get_all("Requires-Python", [])
        if len(python) > 1:
            raise ValueError("Duplicate Requires-Python")
        if python:
            _python_requirement(python[0])
        tag_headers = wheel.get_all("Tag", [])
        internal_tags = set()
        for tag in tag_headers:
            expanded = _tags(tag)
            if internal_tags & expanded:
                raise ValueError("Duplicate internal wheel tags")
            internal_tags.update(expanded)
        if internal_tags != filename_tags:
            raise ValueError("Internal tags disagree with wheel filename")
        pure = _one(wheel, "Root-Is-Purelib")
        if pure not in ("true", "false") or (pure == "true") != all(t[2] == "any" for t in filename_tags):
            raise ValueError("Wheel purelib declaration disagrees with target tags")


def build_sdk_bundle(destination, *, wheelhouse, lock, provenance):
    """Return SHA-256; consume frozen evidence and preserve wheel bytes exactly.

    No fresh provenance/installation claims are made. requirements.txt contains
    canonical pins; hashes are enforced here against the external frozen lock.
    """
    directory = _safe_path(wheelhouse, "directory")
    lock, provenance = _safe_path(lock, "file"), _safe_path(provenance, "file")
    destination = _safe_path(destination, "new")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*\.gluewheels\.zip", destination.name):
        raise ValueError("SDK destination must end in .gluewheels.zip")
    if destination.is_relative_to(directory):
        raise ValueError("Output must be outside the input wheelhouse")
    approved = pins(_safe_path(REQUIREMENTS, "file"))
    if len(approved) != 20:
        raise ValueError("Approved dependency closure must contain 20 packages")
    inventory = _frozen_inventory(lock, provenance, approved)
    files = list(directory.iterdir())
    if {path.name for path in files} != set(inventory):
        raise ValueError("Wheelhouse must contain exactly the frozen wheel files, with no extra content")
    payloads = {"wheels/requirements.txt": "".join(f"{name}=={approved[name]}\n" for name in sorted(approved)).encode("utf-8")}
    for path in files:
        payload = _safe_path(path, "file").read_bytes()
        record = inventory[path.name]
        if hashlib.sha256(payload).hexdigest() != record["sha256"]:
            raise ValueError("Wheel SHA-256 mismatch: " + path.name)
        _validate_wheel(path.name, payload, record)
        payloads["wheels/" + path.name] = payload
    # Validate everything before exclusive creation. ZIP_STORED avoids zlib-version
    # differences and recompression of wheels, which are already ZIP archives.
    with destination.open("xb") as output:
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
            for name, payload in sorted(payloads.items()):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.create_system = 3
                info.external_attr = 0o100644 << 16
                archive.writestr(info, payload)
    return hashlib.sha256(destination.read_bytes()).hexdigest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    for name in ("wheelhouse", "lock", "provenance", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    print(build_sdk_bundle(args.output, wheelhouse=args.wheelhouse, lock=args.lock, provenance=args.provenance))
