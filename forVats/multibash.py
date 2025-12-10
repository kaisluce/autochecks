"""Module to handle batch file submissions and store responses."""

import os

import forVats.batchFile as bf

def log_vat(message: str):
    print(f"[VATS] {message}")

def main(
    work_dir: str,
    token_file_path: str,
    responses: dict,
    specific_files=None,
    progress_callback=None,
):
    """
    Submit batch files and store their responses.

    :param work_dir: Base directory containing the ``data`` folder.
    :param token_file_path: Path to the CSV file used to store tokens.
    :param responses: Mutable mapping updated with submission results.
    :param specific_files: Optional list of specific files to process.
    :param progress_callback: Optional callable receiving progress messages.
    """
    progress = progress_callback or (lambda message: None)
    data_dir = os.path.join(work_dir, "data")

    if specific_files:
        batch_files = sorted(specific_files)
        file_mode = "a"
        write_header = not os.path.exists(token_file_path)
    else:
        batch_files = sorted(os.listdir(data_dir))
        file_mode = "w"
        write_header = True

    log_vat(f"Preparing to submit {len(batch_files)} batch files from {data_dir}")
    with open(token_file_path, file_mode, encoding="utf8") as f:
        if write_header:
            f.write("batch_file,token\n")

        for batch_index, file in enumerate(batch_files):
            batch = bf.submit_batch_file(os.path.join(data_dir, file))
            if batch["status"] == "error":
                log_vat(f"Error with batch file {file}: {batch['message']}")
            else:
                responses[file] = batch
                f.write(f"{file},{batch['data'].get('token')}\n")
                f.flush()
                log_vat(f"Batch file {file} submitted successfully.")
                progress(f"Submitting files ({batch_index + 1}/{len(batch_files)})...")
    log_vat("All batch files submitted.")
    return responses
