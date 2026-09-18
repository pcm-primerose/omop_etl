"""
Build the DB container.

    uv run scripts/build/build_db.py arm64   native image, for local testing
    uv run scripts/build/build_db.py amd64   cluster image, plus a docker-archive tar
    uv run scripts/build/build_db.py sif     apptainer image, from the amd64 tar
    uv run scripts/build/build_db.py all     all of the above
"""

import argparse
import os

from _dist_common import IMAGES_DIR, ROOT, convert_to_sif, docker_build_and_save, print_dist_contents

IMAGE = os.environ.get("IMAGE", "omop-db")
TAG = os.environ.get("TAG", "latest")
DOCKERFILE = ROOT / "docker" / "db.Dockerfile"
ARM_TAR = IMAGES_DIR / f"{IMAGE}-arm64-{TAG}.tar"
TAR = IMAGES_DIR / f"{IMAGE}-amd64-{TAG}.tar"
SIF = IMAGES_DIR / f"{IMAGE}-amd64-{TAG}.sif"


def build_arm64() -> None:
    docker_build_and_save("linux/arm64", f"{IMAGE}:arm64-{TAG}", DOCKERFILE, ROOT, ARM_TAR)


def build_amd64() -> None:
    docker_build_and_save("linux/amd64", f"{IMAGE}:amd64-{TAG}", DOCKERFILE, ROOT, TAR)


def build_sif() -> None:
    if not TAR.exists():
        raise SystemExit(f"missing {TAR}; run 'build_db.py amd64' first")
    convert_to_sif(f"{IMAGE}:amd64-{TAG}", TAR, SIF, f"{IMAGE}-amd64")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["arm64", "amd64", "sif", "all"], nargs="?", default="amd64")
    args = parser.parse_args()

    if args.mode in ("arm64", "all"):
        build_arm64()
    if args.mode in ("amd64", "all"):
        build_amd64()
    if args.mode in ("sif", "all"):
        build_sif()

    print("==> done")
    print_dist_contents(IMAGES_DIR)


if __name__ == "__main__":
    main()
