# infra/manifest.py
from pathlib import Path
from typing import Optional
from .io_core import WriterResult, TableMeta


def build_manifest(
    trial: str,
    run_id: str,
    started_at: str,
    input_path: Path,
    directory: Path,
    fmt: str,
    mode: str,
    result: WriterResult,
    options: Optional[dict] = None,
) -> dict:
    def to_dict(m: TableMeta) -> dict:
        return {"rows": m.rows, "cols": m.cols, "schema": m.schema}

    return {
        "trial": trial,
        "run_id": run_id,
        "started_at": started_at,
        "input": str(input_path.absolute()),
        "output": str(result.main_file.absolute()),
        "directory": str(directory.absolute()),
        "format": fmt,
        "mode": mode,
        "tables": {
            name: {
                **to_dict(meta),
                "file": str(result.table_files.get(name, result.main_file).absolute()),
            }
            for name, meta in result.tables.items()
        },
        "options": options or {},
    }
