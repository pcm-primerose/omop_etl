"""
Assemble the container images needed to upload and run on the cluster:
both images built for amd64 and converted to .sif (dist/images/)
and the run scripts (dist/run/).

    uv run scripts/build/build_dist.py
"""

import argparse
import shutil
import subprocess
from pathlib import Path

from _dist_common import IMAGES_DIR, ROOT, RUN_OUT_DIR, print_dist_contents

RUN_DIR = ROOT / "scripts" / "run"
RUN_SCRIPTS = (
    "podman_run.sh",
    "apptainer_run.sh",
    "setup_podman_persistence.sh",
)


def _run_build_script(name: str, mode: str) -> None:
    subprocess.run(["uv", "run", "python", str(Path(__file__).parent / name), mode], check=True, cwd=ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    _run_build_script("build_etl.py", "amd64")
    _run_build_script("build_etl.py", "sif")
    _run_build_script("build_db.py", "amd64")
    _run_build_script("build_db.py", "sif")

    print("==> copying run scripts")
    shutil.rmtree(RUN_OUT_DIR)
    RUN_OUT_DIR.mkdir()
    for name in RUN_SCRIPTS:
        shutil.copy2(RUN_DIR / name, RUN_OUT_DIR / name)
        (RUN_OUT_DIR / name).chmod(0o755)

    print("==> done")
    print_dist_contents(IMAGES_DIR)
    print_dist_contents(RUN_OUT_DIR)
    print("\nNOTE: dist/ has no athena/ or mappings/ yet: run sync_dist_data.py to add them.")


if __name__ == "__main__":
    main()
