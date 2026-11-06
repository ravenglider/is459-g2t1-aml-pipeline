"""Build a deterministic allowlisted ZIP locally; never upload it or package venvs."""

import argparse
import hashlib
from pathlib import Path
import zipfile

MODULES = ("bq2_sequence_builder.py", "sequence_processing.py", "sequence_source.py",
           "sequence_batch.py", "sequence_staging.py", "sequence_manifest.py", "sequence_aws.py",
           "sequence_s3.py", "glue_sample_preflight.py")
PREFLIGHT_MODULES = MODULES  # Preserve existing callers and the default package.
GENERATION_MODULES = PREFLIGHT_MODULES + (
    "sequence_sample_generation.py", "sequence_sample_staging.py",
    "sequence_sample_run.py", "sequence_glue_runtime.py",
)
PROFILES = {"preflight": PREFLIGHT_MODULES, "generation": GENERATION_MODULES}


def build_package(destination, *, profile="preflight"):
    """Flat module layout matches current imports. Entry script uploaded separately.

    Default preflight retains nine modules; generation adds four orchestrator/
    runtime modules. Neither profile packages SDK dependencies or a new entry job.
    Exclusive creation prevents overwriting existing artifacts. Bytes and ZIP
    metadata are deterministic; this function does not inspect Git or certify
    builder-version evidence. Supply that separately to the eventual manifest.
    """
    if not isinstance(profile, str) or profile not in PROFILES:
        raise ValueError("Unknown packaging profile")
    modules = PROFILES[profile]
    expected = PREFLIGHT_MODULES if profile == "preflight" else GENERATION_MODULES
    if (not isinstance(modules, (tuple, list)) or any(not isinstance(name, str) for name in modules)
            or len(modules) != len(set(modules)) or set(modules) != set(expected)):
        raise ValueError("Profile must contain exactly its unique approved module names")
    root = Path(__file__).resolve().parent
    payloads = []
    for name in modules:
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
    parser.add_argument("--profile", choices=tuple(PROFILES), default="preflight")
    args = parser.parse_args()
    print(build_package(args.output, profile=args.profile))
