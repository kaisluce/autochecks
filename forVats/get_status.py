"""Module to get the status of a VAT validation using a token."""

import requests

def get_status(token):
    """
    function to get the status of a VAT validation using a token.
    
    :param token: token needed to cherch the status
    :type token: str
    """
    url = "https://ec.europa.eu/taxation_customs/vies/rest-api/vat-validation/"+token # GET URL request

    # setting the headers
    headers = {
        'accept' : 'application/json, text/plain, */*',
        'accept-encoding' : 'gzip, deflate, br, zstd',
        'connection' : 'keep-alive',
        'Host' : 'ec.europa.eu',
        'Refer' : 'https://ec.europa.eu/taxation_customs/vies/',
    }
     
    # send the GET request and return the response
    return requests.get(url, headers=headers)