"""Module to concatenate multiple Excel reports into a single file."""

import os
from pathlib import Path

import openpyxl
import pandas as pd

from logger import log_helpers


def main(work_dir: str, logger=None):
    """
    Merge all Excel reports in the ``reports`` directory into a single Excel file.

    :param work_dir: Base directory containing the ``reports`` folder.
    """
    _debug, _log, _warn, _error = log_helpers(logger)

    reports_dir = Path(work_dir) / "reports"
    output_file = Path(work_dir) / "report_concatenated.xlsx"

    all_dfs = []

    for file_name in sorted(os.listdir(reports_dir)):
        if not file_name.lower().endswith((".xls", ".xlsx")):
            continue

        file_path = reports_dir / file_name
        _log(f"Reading {file_name}")

        df = pd.read_excel(file_path, dtype=str)
        df["__source_file__"] = file_name
        all_dfs.append(df)

    if not all_dfs:
        _warn("Aucun fichier excel a concatener.")
        return

    merged = pd.concat(all_dfs, ignore_index=True)
    merged.to_excel(output_file, index=False)
    _log(f"Fichier final cree : {output_file}")
