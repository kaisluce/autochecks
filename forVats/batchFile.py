"""
Module to handle batch file submission with rejection handling.
"""

import forVats.forceHTTP as fh
import forVats.get_status as gs
import time

def log_vat(message: str):
    print(f"[VATS] {message}")

def submit_batch_file(batch_file):
    """
    Function that submits a single batch file and handles rejections by retrying.
    
    :param batch_file: File path of the batch file to submit
    """
    # main loop to handle rejections
    while True:
        log_vat(f"Submitting batch file {batch_file}")
        # sends the batch file
        upl = fh.upload_batch(batch_file)
        # checks for HTTP errors
        if upl.status_code != 200:
            log_vat(f"HTTP error during upload: {upl.status_code}")
            return {
                "status": "error",
                "message": f"HTTP error {upl.status_code}",
                "code": upl.status_code
            }
        
        # retrieves the token from the upload response
        token = upl.json().get("token")

        # handles missing token
        if not token:
            log_vat("No token received from upload response")
            return {
                "status": "error",
                "message": "No token received",
                "code": 400
            }

        # retrieves the status using the token
        status_resp = gs.get_status(token)

        # handles HTTP errors when checking status
        if status_resp.status_code != 200:
            log_vat(f"HTTP error during status check: {status_resp.status_code}")
            return {
                "status": "error",
                "message": f"HTTP error {status_resp.status_code}",
                "code": status_resp.status_code
            }
        
        # checks if the batch was rejected
        status = status_resp.json().get("status")

        if status.upper() == "PROCESSING":
            # if not rejected, return the status
            log_vat(f"Batch {batch_file} accepted, token {token}")
            return {
                "status": "PROCESSING",
                "data": status_resp.json()
            }
