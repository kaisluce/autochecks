"""
Module to handle batch file submission with rejection handling.
"""

import forVats.forceHTTP as fh
import forVats.get_status as gs
import time
from requests.exceptions import RequestException

from logger import log_helpers


def submit_batch_file(batch_file, logger=None, max_retries: int = 5, retry_delay: int = 5):
    """
    Submit one VAT batch file and retry on transient failures or rejections.
    
    :param batch_file: File path of the batch file to submit
    :param max_retries: Maximum submission attempts before returning an error
    :param retry_delay: Delay in seconds between attempts
    """
    _debug, _log, _warn, _error = log_helpers(logger)
    attempt = 0
    while attempt < max_retries:
        try:
            _log(f"Submitting batch file {batch_file} (attempt {attempt}/{max_retries})")
            # sends the batch file
            upl = fh.upload_batch(batch_file)
            # checks for HTTP errors (retry 5xx, fail fast on 4xx)
            if upl.status_code >= 500:
                _warn(f"HTTP server error during upload: {upl.status_code}")
                attempt += 1
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                return {
                    "status": "error",
                    "message": f"Upload failed after retries (HTTP {upl.status_code})",
                    "code": upl.status_code
                }
            if upl.status_code != 200:
                _warn(f"HTTP client error during upload: {upl.status_code}")
                attempt += 1
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                return {
                    "status": "error",
                    "message": f"HTTP error {upl.status_code}",
                    "code": upl.status_code
                }
            
            # retrieves the token from the upload response
            try:
                token = upl.json().get("token")
            except ValueError:
                _warn("Upload response is not valid JSON")
                attempt += 1
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                return {
                    "status": "error",
                    "message": "Upload response is not valid JSON",
                    "code": 502
                }

            # handles missing token
            if not token:
                _warn("No token received from upload response")
                attempt += 1
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                return {
                    "status": "error",
                    "message": "No token received",
                    "code": 400
                }

            # retrieves the status using the token
            status_resp = gs.get_status(token)

            # handles HTTP errors when checking status (retry 5xx, fail fast on 4xx)
            if status_resp.status_code >= 500:
                _warn(f"HTTP server error during status check: {status_resp.status_code}")
                attempt += 1
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                return {
                    "status": "error",
                    "message": f"Status check failed after retries (HTTP {status_resp.status_code})",
                    "code": status_resp.status_code
                }
            if status_resp.status_code != 200:
                _warn(f"HTTP client error during status check: {status_resp.status_code}")
                return {
                    "status": "error",
                    "message": f"HTTP error {status_resp.status_code}",
                    "code": status_resp.status_code
                }
            
            try:
                status_payload = status_resp.json()
            except ValueError:
                _warn("Status response is not valid JSON")
                attempt += 1
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                return {
                    "status": "error",
                    "message": "Status response is not valid JSON",
                    "code": 502
                }

            # checks if the batch was accepted
            status = str(status_payload.get("status", "")).upper()
            _log(f"Received status: {status or '(empty)'}")
            if status.upper() == "PROCESSING":
                # if not rejected, return the status
                _log(f"Batch {batch_file} accepted, token {token}")
                return {
                    "status": "PROCESSING",
                    "data": status_payload
                }

            # Retry explicit rejections / unknown transient statuses only up to max_retries.
            if status == "REJECTED":
                _warn(f"Batch {batch_file} rejected by API.")
            else:
                _warn(f"Unexpected API status for {batch_file}: {status or '(empty)'}")

            if attempt < max_retries:
                time.sleep(retry_delay)
                continue
            return {
                "status": "error",
                "message": f"Batch not accepted after {max_retries} attempts (last status: {status or 'EMPTY'})",
                "code": 409
            }

        except RequestException as exc:
            _warn(f"Network error during batch submission: {exc}")
            attempt += 1
            if attempt < max_retries:
                time.sleep(retry_delay)
                continue
            return {
                "status": "error",
                "message": f"Network error after retries: {exc}",
                "code": 503
            }
        except OSError as exc:
            _warn(f"Local file error for {batch_file}: {exc}")
            return {
                "status": "error",
                "message": f"Local file error: {exc}",
                "code": 500
            }
