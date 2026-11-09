"""Separate deterministic full-generation profile; sample packager unchanged."""
import argparse
import hashlib
from pathlib import Path
import zipfile

from package_glue_modules import GENERATION_MODULES

FULL_MODULES = GENERATION_MODULES + (
    "glue_sample_generation.py", "sequence_full_source.py",
    "sequence_full_staging.py", "sequence_full_generation.py",
    "sequence_full_publication.py",
)


def build_full_package(destination):
    root = Path(__file__).resolve().parent
    if len(FULL_MODULES) != 18 or len(set(FULL_MODULES)) != 18:
        raise ValueError("Expected exactly 18 full-pipeline modules")
    destination = Path(destination)
    if destination.suffix != ".zip" or destination.is_symlink():
        raise ValueError("Fresh ZIP file required")
    payloads = []
    for name in sorted(FULL_MODULES):
        path = root/name
        if path.is_symlink() or not path.is_file():
            raise ValueError("Missing or unsafe full module")
        payloads.append((name, path.read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8")))
    with zipfile.ZipFile(destination, "x", compression=zipfile.ZIP_STORED) as archive:
        for name, payload in payloads:
            info = zipfile.ZipInfo(name, date_time=(1980,1,1,0,0,0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, payload)
    return hashlib.sha256(destination.read_bytes()).hexdigest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--output", required=True)
    print(build_full_package(parser.parse_args().output))
