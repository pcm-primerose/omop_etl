"""Shared helpers for build_etl.py, build_db.py & build_dist.py"""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DIST = ROOT / "dist"
IMAGES_DIR = DIST / "images"
RUN_OUT_DIR = DIST / "run"
CACHE = ROOT / ".dist-cache"

for _dir in (DIST, IMAGES_DIR, RUN_OUT_DIR, CACHE):
    _dir.mkdir(exist_ok=True)

# singularity-in-docker, so nothing needs installing locally and the
# conversion doesn't depend on this machine's arch
SING_IMAGE = "quay.io/singularity/singularity:v3.11.4"


def run(cmd: list[str]) -> None:
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True)


def docker_build_and_save(platform: str, tag: str, dockerfile: Path, context: Path, tar_path: Path, *, no_cache: bool = False) -> None:
    print(f"==> docker image, {platform}")
    cmd = ["docker", "buildx", "build", "--platform", platform, "-t", tag, "-f", str(dockerfile)]
    if no_cache:
        cmd.append("--no-cache")
    cmd += ["--load", str(context)]
    run(cmd)
    print(f"==> docker archive -> {tar_path}")
    run(["docker", "save", tag, "-o", str(tar_path)])


def _image_digest(tag: str) -> str:
    result = subprocess.run(["docker", "inspect", "--format", "{{.Id}}", tag], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def convert_to_sif(tag: str, tar_path: Path, sif_path: Path, cache_key: str) -> None:
    """
    Skip the conversion if the image's current digest matches what was last
    converted for `cache_key`
    """
    digest_file = CACHE / f"{cache_key}.digest"
    digest = _image_digest(tag)

    if sif_path.exists() and digest_file.exists() and digest_file.read_text().strip() == digest:
        print(f"==> {sif_path} already up to date (image unchanged since last conversion), skipping")
        return

    work_dir = tar_path.parent  # tar_path and sif_path always share a parent (IMAGES_DIR)

    # a docker volume is ext4 inside docker's linux vm, while a bind mount
    # would be APFS/whatever-the-host-is through a translation layer, and
    # singularity's build needs real linux filesystem semantics (xattrs, ownership)
    sing_vol = f"{cache_key}_sing_tmp"
    run(["docker", "volume", "create", sing_vol])

    print(f"==> apptainer image -> {sif_path}")
    try:
        run(
            [
                "docker",
                "run",
                "--rm",
                "--platform",
                "linux/amd64",
                "--privileged",
                "-v",
                f"{work_dir}:/work",
                "-v",
                f"{sing_vol}:/sing-tmp",
                "-e",
                "SINGULARITY_TMPDIR=/sing-tmp",
                "-e",
                "SINGULARITY_CACHEDIR=/sing-tmp/cache",
                SING_IMAGE,
                "build",
                "--force",
                "--tmpdir",
                "/sing-tmp",
                f"/work/{sif_path.name}",
                f"docker-archive:/work/{tar_path.name}",
            ]
        )
    finally:
        subprocess.run(["docker", "volume", "rm", sing_vol], capture_output=True)

    digest_file.write_text(digest)


def print_dist_contents(dir_path: Path) -> None:
    print(f"{dir_path.relative_to(ROOT)}/:")
    for f in sorted(dir_path.iterdir()):
        if f.is_file():
            size = subprocess.run(["du", "-h", str(f)], check=True, capture_output=True, text=True).stdout.split()[0]
            print(f"  {f.name:<34} {size}")
