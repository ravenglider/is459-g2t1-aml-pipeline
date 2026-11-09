"""Separate deterministic 22-module chunked job ZIP; entry script is separate."""
import argparse
import hashlib
from pathlib import Path
import zipfile
from package_full_generation import FULL_MODULES

MODULES=FULL_MODULES+("glue_hi_growth_probe.py","sequence_chunked_generation.py","sequence_chunked_staging.py","sequence_owner_date.py")


def build_package(destination,*,batched=False):
    if len(MODULES)!=len(set(MODULES)) or len(MODULES)!=22:raise ValueError("Expected 22 modules")
    path=Path(destination)
    if path.suffix!=".zip" or path.is_symlink():raise ValueError("Fresh ZIP required")
    for parent in path.absolute().parents:
        if parent.is_symlink():raise ValueError("Unsafe destination parent")
    payloads=[]
    modules=MODULES+("sequence_batched_kernel.py","sequence_batched_staging.py") if batched else MODULES
    if len(modules)!=len(set(modules)) or len(modules)!=(24 if batched else 22):raise ValueError("Unexpected package closure")
    for name in sorted(modules):
        source=Path(__file__).parent/name
        if source.is_symlink() or not source.is_file():raise ValueError("Missing/unsafe module")
        payloads.append((name,source.read_text(encoding="utf-8").replace("\r\n","\n").encode()))
    with zipfile.ZipFile(path,"x",compression=zipfile.ZIP_STORED) as out:
        for name,raw in payloads:
            info=zipfile.ZipInfo(name,date_time=(1980,1,1,0,0,0));info.create_system=3;info.external_attr=0o100644<<16
            out.writestr(info,raw)
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__=="__main__":
    p=argparse.ArgumentParser(allow_abbrev=False);p.add_argument("--output",required=True);p.add_argument("--batched",action="store_true")
    args=p.parse_args();print(build_package(args.output,batched=args.batched))
