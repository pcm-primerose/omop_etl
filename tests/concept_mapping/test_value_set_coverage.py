"""
Static check that every value_set name passed to `.resolve(...)`
somewhere in src/ actually exists in mappings/structural.csv or
mappings/static.csv.
"""

import ast
import csv
from pathlib import Path

from omop_etl.env_config import PROJECT_ROOT

SRC_DIR = PROJECT_ROOT / "src" / "omop_etl"
STRUCTURAL_CSV = PROJECT_ROOT / "mappings" / "structural.csv"
STATIC_CSV = PROJECT_ROOT / "mappings" / "static.csv"


def _resolved_value_sets(py_file: Path) -> set[str]:
    """Every literal string passed as the first positional arg to a `.resolve(...)` call."""
    tree = ast.parse(py_file.read_text(), filename=str(py_file))
    names: set[str] = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "resolve"):
            continue
        if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
            names.add(node.args[0].value)
    return names


def _known_value_sets(csv_path: Path) -> set[str]:
    known: set[str] = set()
    with csv_path.open(newline="") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            known.add(next(csv.reader([line]))[0].strip())
    return known


def test_every_resolved_value_set_exists_in_mapping_files():
    used: set[str] = set()
    for py_file in SRC_DIR.rglob("*.py"):
        used |= _resolved_value_sets(py_file)

    known = _known_value_sets(STRUCTURAL_CSV) | _known_value_sets(STATIC_CSV)

    missing = sorted(used - known)
    assert not missing, f"value_set(s) passed to .resolve(...) but not found in structural.csv/static.csv: {missing}"
