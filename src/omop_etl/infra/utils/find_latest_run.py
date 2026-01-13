from pathlib import Path
from typing import Literal, Union


def find_latest_run_output(
    module: Literal["preprocessed", "harmonized"],
    trial: str = "impress",
    fmt: str = "csv",
    mode: Literal["wide", "norm"] | None = None,
    data_root: Path | None = None,
) -> Union[Path, dict[str, Path]] | None:
    """
    Find the most recent output from a pipeline module.

    Args:
        module: What module's output to find (preprocessed, harmonized)
        trial: Trial name (impress)
        fmt: File formats (csv, parquet, tsv, json if harmonized wide)
        mode: For harmonized only - 'wide' (single file) or 'norm' (multiple files)
        data_root: Base data directory

    Returns:
        - For preprocessing: Path to the preprocessed file
        - For harmonized wide: Path to the wide harmonized file
        - For harmonized norm: Dict mapping table names to their file paths
    """
    if data_root is None:
        from omop_etl.config import DATA_ROOT

        data_root = DATA_ROOT

    runs_dir = data_root / "runs"

    if not runs_dir.exists():
        return None

    # find all timestamped run directories
    all_run_dirs = [_dir for _dir in runs_dir.iterdir() if _dir.is_dir()]
    if not all_run_dirs:
        return None

    if module == "preprocessed":
        # find runs that have preprocessed output for this trial
        valid_runs = []
        for run_dir in all_run_dirs:
            format_dir = run_dir / "preprocessed" / trial / "preprocessed" / fmt
            if format_dir.exists():
                files = list(format_dir.glob(f"*_preprocessed.{fmt}"))
                if files:
                    valid_runs.append((run_dir, files[0]))

        if not valid_runs:
            return None

        latest_run, latest_file = max(valid_runs, key=lambda x: x[0].stat().st_mtime)
        return latest_file

    if module == "harmonized":
        if mode is None:
            raise ValueError("Must specify mode='wide' or mode='norm' for harmonized data")

        # find runs with harmonized output for this trial
        valid_runs = []
        for run_dir in all_run_dirs:
            format_dir = run_dir / "harmonized" / trial / f"harmonized_{mode}" / fmt
            if format_dir.exists():
                if mode == "wide":
                    files = list(format_dir.glob(f"*_harmonized_wide.{fmt}"))
                    if files:
                        valid_runs.append((run_dir, files[0]))
                elif mode == "norm":
                    files = [f for f in format_dir.glob(f"*.{fmt}") if not f.name.endswith(".log")]
                    if files:
                        file_dict = {f.stem: f for f in files}
                        valid_runs.append((run_dir, file_dict))

        if not valid_runs:
            return None

        latest_run, result = max(valid_runs, key=lambda x: x[0].stat().st_mtime)
        return result

    return None
