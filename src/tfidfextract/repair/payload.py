import os
from pathlib import Path

import polars as pl


def validate_single_pair(in_data: tuple[str, str, Path]):
    _, _, path = in_data
    validate_single_path(path)


def validate_single_path(path: Path):
    try:
        pl.read_parquet(path)
    except Exception:
        os.remove(path)
