"""Module to check the completion status of batch submissions."""

import os
import time

import forVats.batchFile as bf
import forVats.downloadrepport as do
import forVats.get_status as gs

def log_vat(message: str):
    print(f"[VATS] {message}")

def main(responses: dict, work_dir: str, token_file: str, progress_callback=None):
    """
    Poll batch submissions until all are completed.

    :param responses: Mutable mapping of batch file names to their status data.
    :param work_dir: Base directory containing ``data`` and ``reports`` folders.
    :param token_file: Path to the CSV file storing batch tokens.
    :param progress_callback: Optional callable receiving progress messages.
    """
    progress = progress_callback or (lambda message: None)

    data_dir = os.path.join(work_dir, "data")
    reports_dir = os.path.join(work_dir, "reports")

    log_vat("Checking completion...")
    tries = 0
    log_vat(f"Initial responses: {responses}")
    os.makedirs(reports_dir, exist_ok=True)

    while True:
        tries += 1
        completed = 0
        done = True

        log_vat(f"Poll iteration {tries}")
        filenb = 0
        for file, response in responses.items():
            status_str = str(response.get("status", "")).upper()

            log_vat(f"Checking file {file}: status {status_str}")
            if status_str == "REJECTED":
                batch_path = os.path.join(data_dir, file)
                log_vat(f"Batch {file} rejected. Resubmitting...")
                retry_resp = bf.submit_batch_file(batch_path)
                if retry_resp.get("status") == "error":
                    log_vat(f"Error resubmitting {file}: {retry_resp.get('message')}")
                    done = False
                    continue
                responses[file] = retry_resp
                try:
                    with open(token_file, "a", encoding="utf8") as f:
                        f.write(f"{file},{retry_resp['data'].get('token')}\n")
                except Exception as write_exc:  # pragma: no cover - defensive log
                    log_vat(f"Could not append new token for {file}: {write_exc}")
                done = False
                progress(f"Resubmitted {file}, waiting for completion...")
                continue

            if status_str == "PROCESSING":
                token = response.get("data", {}).get("token")
                status = gs.get_status(token)

                if status.status_code != 200:
                    log_vat(f"Error checking status for {file}: HTTP {status.status_code}")
                    continue

                status_json = status.json()
                responses[file]["data"] = status_json

                log_vat(f"New status: {status_json.get('status', '(no status)').upper()}")

                if status_json.get("status", "").upper() == "COMPLETED" or status_json.get("percentage") == 100.0:
                    log_vat(f"Batch file {file} completed.")
                    responses[file]["status"] = "COMPLETED"

                    do.main(token, os.path.join(reports_dir, f"{os.path.splitext(file)[0]}_report.xlsx"))
                    progress(f"Downloaded report for file {filenb + 1}/{len(responses)}.")
                else:
                    done = False
                    if completed == filenb:
                        percentage = status_json.get("percentage")
                        progress(f"Waiting for file {completed + 1}/{len(responses)} to complete... ({percentage}%)")
            else:
                completed += 1
            filenb += 1
        time.sleep(10)

        if done:
            log_vat("All batch files completed.")
            progress("All batch files completed.")
            break
