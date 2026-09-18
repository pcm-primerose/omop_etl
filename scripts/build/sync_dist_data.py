"""
Refresh the Athena bundle and/or mappings/ to dist/.

    uv run scripts/build/sync_dist_data.py              refreshes both
    uv run scripts/build/sync_dist_data.py --athena     just the athena bundle
    uv run scripts/build/sync_dist_data.py --mappings   just mappings/
"""

import argparse
import os
import shutil
from pathlib import Path

from _dist_common import DIST, ROOT

ATHENA_DIR = Path(os.environ["ATHENA_DIR"]) if os.environ.get("ATHENA_DIR") else ROOT / ".data" / "athena"
MAPPING_DIR = Path(os.environ["MAPPING_DIR"]) if os.environ.get("MAPPING_DIR") else ROOT / "mappings"


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst, symlinks=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--athena", action="store_true", help="only refresh the athena bundle")
    parser.add_argument("--mappings", action="store_true", help="only refresh mappings/")
    args = parser.parse_args()
    do_athena = args.athena or not args.mappings
    do_mappings = args.mappings or not args.athena

    if do_athena:
        print(f"==> copying athena bundle from {ATHENA_DIR}")
        _copy_tree(ATHENA_DIR, DIST / "athena")
    if do_mappings:
        print(f"==> copying mappings from {MAPPING_DIR}")
        _copy_tree(MAPPING_DIR, DIST / "mappings")

    print("==> done")


if __name__ == "__main__":
    main()
