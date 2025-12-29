"""Module to check the completion status of batch submissions."""

import os
import time

import forVats.batchFile as bf
import forVats.downloadrepport as do
import forVats.get_status as gs


def _log_helpers(logger=None):
    def _info(msg):
        if logger is None:
            print(f"[VATS] {msg}")
        elif hasattr(logger, "log"):
            logger.log(msg)
        elif hasattr(logger, "info"):
            logger.info(msg)
        else:
            logger(msg)

    def _warn(msg):
        if logger is None:
            print(f"[VATS][WARN] {msg}")
        elif hasattr(logger, "warn"):
            logger.warn(msg)
        elif hasattr(logger, "warning"):
            logger.warning(msg)
        else:
            _info(f"[WARN] {msg}")
    return _info, _warn


def main(responses: dict, work_dir: str, token_file: str, progress_callback=None, logger=None):
    """
    Poll batch submissions until all are completed.

    :param responses: Mutable mapping of batch file names to their status data.
    :param work_dir: Base directory containing ``data`` and ``reports`` folders.
    :param token_file: Path to the CSV file storing batch tokens.
    :param progress_callback: Optional callable receiving progress messages.
    """
    _info, _warn = _log_helpers(logger)
    progress = progress_callback or (lambda message: None)

    data_dir = os.path.join(work_dir, "data")
    reports_dir = os.path.join(work_dir, "reports")

    _info("Checking completion...")
    tries = 0
    _info(f"Initial responses: {responses}")
    os.makedirs(reports_dir, exist_ok=True)

    while True:
        tries += 1
        completed = 0
        done = True

        _info(f"Poll iteration {tries}")
        filenb = 0
        for file, response in responses.items():
            status_str = str(response.get("status", "")).upper()

            _info(f"Checking file {file}: status {status_str}")
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
                status = gs.get_status(token)

                if status.status_code != 200:
                    _warn(f"Error checking status for {file}: HTTP {status.status_code}")
                    continue

                status_json = status.json()
                responses[file]["data"] = status_json

                _info(f"New status: {status_json.get('status', '(no status)').upper()}")

                if status_json.get("status", "").upper() == "COMPLETED" or status_json.get("percentage") == 100.0:
                    _info(f"Batch file {file} completed.")
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
            _info("All batch files completed.")
            progress("All batch files completed.")
            break
