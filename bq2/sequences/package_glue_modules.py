"""Build a deterministic allowlisted ZIP locally; never upload it or package venvs."""

import argparse
import hashlib
from pathlib import Path
import zipfile

MODULES = ("bq2_sequence_builder.py", "sequence_processing.py", "sequence_source.py",
           "sequence_batch.py", "sequence_staging.py", "sequence_manifest.py", "sequence_aws.py",
           "sequence_s3.py")


def build_package(destination):
    """Flat module layout matches current imports. Entry script uploaded separately.

    Exclusive creation prevents overwriting existing artifacts. Bytes and ZIP
    metadata are deterministic; this function does not inspect Git or certify
    builder-version evidence. Supply that separately to the eventual manifest.
    """
    root = Path(__file__).resolve().parent
    payloads = []
    for name in MODULES:
        path = root / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("Missing/unsafe allowlisted module: " + name)
        # Canonical LF avoids differing archives from Windows/WSL checkout endings.
        payloads.append((name, path.read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8")))
    destination = Path(destination)
    if destination.suffix != ".zip":
        raise ValueError("Package destination must end in .zip")
    with zipfile.ZipFile(destination, "x", compression=zipfile.ZIP_STORED) as archive:
        for name, content in sorted(payloads):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, content)
    return hashlib.sha256(destination.read_bytes()).hexdigest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    print(build_package(parser.parse_args().output))
