"""
Module to handle batch file uploads to the VAT validation service.
"""

import requests

def upload_batch(file_path):
    """
    Function that uploads a batch file to the VAT
    validation service using a http request.
    
    :param file_path: Path to the batch file to upload
    :type file_path: str
    """

    url = "https://ec.europa.eu/taxation_customs/vies/rest-api/vat-validation"  # POST URL request

    # setting the headers

    headers = {
        'accept' : 'application/json, text/plain, */*',
        'accept-encoding' : 'gzip, deflate, br, zstd',
        'Origin' : 'https://ec.europa.eu',
        'Refer' : 'https://ec.europa.eu/taxation_customs/vies/'
    }

    # set the files to be sent in the request
    files = {
        "fileToUpload": open(file_path, "rb"),
    }

    # send the POST request and return the response
    return requests.post(url, headers=headers, files=files)