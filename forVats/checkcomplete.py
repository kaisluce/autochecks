"""Module to check the completion status of batch submissions."""

import os
import time

import forVats.batchFile as bf
import forVats.downloadrepport as do
import forVats.get_status as gs
from requests.exceptions import RequestException

from logger import log_helpers


def main(responses: dict, work_dir: str, token_file: str, progress_callback=None, logger=None):
    """
    Poll batch submissions until all are completed.

    :param responses: Mutable mapping of batch file names to their status data.
    :param work_dir: Base directory containing ``data`` and ``reports`` folders.
    :param token_file: Path to the CSV file storing batch tokens.
    :param progress_callback: Optional callable receiving progress messages.
    """
    _debug, _log, _warn, _error = log_helpers(logger)
    progress = progress_callback or (lambda message: None)

    data_dir = os.path.join(work_dir, "data")
    reports_dir = os.path.join(work_dir, "reports")

    _log("Checking completion...")
    tries = 0
    _log(f"Initial responses: {responses}")
    os.makedirs(reports_dir, exist_ok=True)

    while True:
        tries += 1
        completed = 0
        done = True

        _log(f"Poll iteration {tries}")
        filenb = 0
        for file, response in responses.items():
            status_str = str(response.get("status", "")).upper()

            _log(f"Checking file {file}: status {status_str}")
            if status_str == "REJECTED":
                batch_path = os.path.join(data_dir, file)
                _warn(f"Batch {file} rejected. Resubmitting...")
                retry_resp = bf.submit_batch_file(batch_path, logger=logger)
                if retry_resp.get("status") == "error":
                    _warn(f"Error resubmitting {file}: {retry_resp.get('message')}")
                    done = False
                    continue
                responses[file] = retry_resp
                try:
                    with open(token_file, "a", encoding="utf8") as f:
                        f.write(f"{file},{retry_resp['data'].get('token')}\n")
                except Exception as write_exc:  # pragma: no cover - defensive log
                    _warn(f"Could not append new token for {file}: {write_exc}")
                done = False
                progress(f"Resubmitted {file}, waiting for completion...")
                continue

            if status_str == "PROCESSING":
                token = response.get("data", {}).get("token")
                try:
                    status = gs.get_status(token)
                except RequestException as req_exc:
                    _warn(f"Network error checking status for {file}: {req_exc}")
                    done = False
                    continue

                if status.status_code != 200:
                    _warn(f"Error checking status for {file}: HTTP {status.status_code}")
                    done = False
                    continue

                try:
                    status_json = status.json()
                except ValueError as json_exc:
                    _warn(f"Invalid JSON status payload for {file}: {json_exc}")
                    done = False
                    continue
                responses[file]["data"] = status_json

                _log(f"New status: {status_json.get('status', '(no status)').upper()}")

                if status_json.get("status", "").upper() == "COMPLETED" or status_json.get("percentage") == 100.0:
                    try:
                        do.main(token, os.path.join(reports_dir, f"{os.path.splitext(file)[0]}_report.xlsx"))
                    except RequestException as req_exc:
                        _warn(f"Network error downloading report for {file}: {req_exc}")
                        done = False
                        continue
                    except OSError as io_exc:
                        _warn(f"File error while saving report for {file}: {io_exc}")
                        done = False
                        continue

                    _log(f"Batch file {file} completed.")
                    responses[file]["status"] = "COMPLETED"
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
            _log("All batch files completed.")
            progress("All batch files completed.")
            break
