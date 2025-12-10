"""Processing pipeline for VAT batch checks without tkinter dependencies."""

import os
import time

import openpyxl
import pandas as pd

import forVats.checkcomplete as cc
import forVats.concat as ct
import forVats.multibash as mb
import forVats.reformate as rf

def log_vat(message: str):
    print(f"[VATS] {message}")

def load_tokens_from_csv(token_path: str) -> dict:
    """Load previously saved tokens so continuation runs can poll all batches."""
    responses = {}

    if not os.path.exists(token_path):
        log_vat(f"{token_path} not found. Assuming no files were processed yet.")
        return responses

    try:
        token_df = pd.read_csv(token_path)
    except Exception as exc:  # pragma: no cover - defensive logging
        log_vat(f"Unable to read {token_path}: {exc}")
        return responses

    for _, row in token_df.iterrows():
        batch_file = row.get("batch_file")
        token = row.get("token")
        if pd.isna(batch_file) or pd.isna(token):
            continue
        responses[str(batch_file)] = {
            "status": "PROCESSING",
            "data": {"token": str(token)},
        }

    log_vat(f"Loaded {len(responses)} tokens from {token_path}.")
    return responses


def process(
    df: pd.DataFrame,
    vat_column: str,
    output_dir: str,
    requester_vat: str = "FR75383926409",
    progress_callback=None,
):
    """
    Execute the full processing pipeline for a new VAT batch check.

    :param filepath: Path to the source Excel file.
    :param vat_column: Name of the column containing VAT numbers.
    :param output_dir: Directory where intermediate and final files will be written.
    :param requester_vat: VAT number of the requester.
    :param progress_callback: Optional callable receiving progress messages.
    """
    progress = progress_callback or (lambda message: None)
    os.makedirs(output_dir / "data", exist_ok=True)

    log_vat(f"VAT process started for column '{vat_column}' in {output_dir}")
    progress("Step 1: Reformatting data")
    rf.reformate(df, vat_column, output_dir, requester_vat, progress_callback=progress)

    token_file = os.path.join(output_dir, "tokens.csv")
    responses = {}

    progress("Step 2: Submitting batch files...")
    mb.main(output_dir, token_file, responses, progress_callback=progress)

    progress("Step 3: Checking completion...")
    time.sleep(1)
    cc.main(responses, output_dir, token_file, progress_callback=progress)

    progress("Step 4: Concatenating results...")
    time.sleep(1)
    ct.main(output_dir)

    progress("Processing completed!")
    log_vat("VAT processing completed")
    return responses


def continue_process(directory: str, progress_callback=None):
    """
    Continue a previously started processing pipeline from an existing directory.

    :param directory: Working directory containing ``data`` and ``tokens.csv``.
    :param progress_callback: Optional callable receiving progress messages.
    """
    progress = progress_callback or (lambda message: None)

    progress("Continuing process from selected directory.")

    data_dir = os.path.join(directory, "data")
    token_file = os.path.join(directory, "tokens.csv")

    existing_responses = load_tokens_from_csv(token_file)
    uploaded_files = list(existing_responses.keys())

    all_files = [f for f in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, f))]
    files_to_upload = [f for f in all_files if f not in uploaded_files]

    if files_to_upload:
        progress(f"Submitting files (0/{len(files_to_upload)})...")
        mb.main(directory, token_file, existing_responses, specific_files=files_to_upload, progress_callback=progress)
    else:
        progress("No new files to upload.")

    progress("Step 2: Checking completion...")
    cc.main(existing_responses, directory, token_file, progress_callback=progress)

    progress("Step 3: Concatenating results...")
    ct.main(directory)

    progress("Processing completed!")
    return existing_responses


def main(
    filepath: str,
    vat_column: str,
    output_dir: str,
    requester_vat: str = "FR75383926409",
    progress_callback=None,
):
    """
    Alias for ``process`` to preserve the previous entry point name.
    """
    return process(filepath, vat_column, output_dir, requester_vat, progress_callback)
