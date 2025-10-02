from pathlib import Path
from .run_context import RunContext


def resolve_artifact_dir(root: Path, ctx: RunContext) -> Path:
    out = root / ctx.artifact_dir
    out.mkdir(parents=True, exist_ok=True)
    return out


def table_path(dir: Path, name: str, ext: str = ".csv") -> Path:
    return (dir / name).with_suffix(ext)
