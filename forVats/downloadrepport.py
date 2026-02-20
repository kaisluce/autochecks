"""Module to download VAT validation report documents using a token."""

import csv
import os

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
    return requests.get(url, headers=headers, timeout=30)


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
        f.write(resp.content)

    # return the output path for confirmation
    return output_path


def _iter_tokens(token_csv_path):
    with open(token_csv_path, newline="", encoding="utf8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            token = (row.get("token") or "").strip()
            batch_file = (row.get("batch_file") or "").strip()
            if token:
                yield batch_file, token


if __name__ == "__main__":
    report_dir = r"Z:\MDM\998_CHecks\AUTOCHECKS\2026-01-14_10-01_REPORT\vat"
    token_csv = os.path.join(report_dir, "tokens.csv")
    reports_dir = os.path.join(report_dir, "reports")
    os.makedirs(reports_dir, exist_ok=True)

    if os.path.exists(token_csv):
        for batch_file, token in _iter_tokens(token_csv):
            output_name = f"{os.path.splitext(batch_file)[0]}_report.xlsx" if batch_file else f"{token}_report.xlsx"
            main(token, os.path.join(reports_dir, output_name))
    else:
        token = input("Token: ").strip()
        output_path = os.path.join(reports_dir, f"{token}_report.xlsx")
        main(token, output_path)
