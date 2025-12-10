"""Module to download VAT validation report documents using a token."""

import requests

def get_document(token):
    """
    Retrieve the Excel result file for a given token.
    Returns the requests.Response object (binary content in response.content).

    :param token: The token identifying the VAT validation report.
    :return: requests.Response containing the binary file.
    """
    # set the URL with the provided token
    url = (
        "https://ec.europa.eu/taxation_customs/vies/rest-api/"
        "vat-validation-report/" + token
    )

    # set the headers for the request
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,image/apng,*/*;q=0.8,"
                  "application/signed-exchange;v=b3;q=0.7",
        "accept-encoding": "gzip, deflate, br, zstd",
        "connection": "keep-alive",
        "Host": "ec.europa.eu",
        "Referer": "https://ec.europa.eu/taxation_customs/vies/",
    }

    # send the GET request and return the response
    return requests.get(url, headers=headers)


def main(token, output_path):
    """
    Use get_document(token) to retrieve the file and save it to disk.
    Returns the path to the saved file.

    :param token: The token identifying the VAT validation report.
    :param output_path: The file path where the document will be saved.
    :return: The path to the saved file.
    """
    resp = get_document(token)
    resp.raise_for_status()  # handle HTTP errors

    # write the binary content to the specified output path
    with open(output_path, "wb") as f:
        print(resp)
        f.write(resp.content)

    # return the output path for confirmation
    return output_path
