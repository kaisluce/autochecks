import base64
import os
from getpass import getpass
from pathlib import Path
from typing import Optional

import msal
import requests

GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]


def _load_pfx_certificate(pfx_path: str, password: Optional[str]):
    """
    Load a PFX and return a dict usable by MSAL (private key PEM + thumbprint).
    Tries the provided password, then empty/None for password-less PFX.
    If no password is provided and empty/None fail, prompt once.
    """
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        pkcs12,
    )

    pfx_bytes = Path(pfx_path).expanduser().read_bytes()

    def _try_load(pwd: Optional[str]):
        return pkcs12.load_key_and_certificates(
            pfx_bytes,
            pwd.encode() if isinstance(pwd, str) and pwd != "" else None,
        )

    attempts = []
    if password is not None:
        attempts.append(password)
    attempts.extend(["", None])

    private_key = cert = None
    for candidate in attempts:
        try:
            private_key, cert, _ = _try_load(candidate)
            break
        except ValueError:
            private_key = cert = None

    # If not loaded yet and no password was provided, prompt once
    if not private_key or not cert:
        if password is None:
            prompt_pwd = getpass(f"Mot de passe pour {pfx_path}: ")
            try:
                private_key, cert, _ = _try_load(prompt_pwd)
            except ValueError:
                private_key = cert = None

    if not private_key or not cert:
        raise ValueError(
            "Impossible de charger le PFX (mot de passe invalide ou fichier corrompu). "
            f"PFX: {pfx_path}"
        )

    private_key_pem = private_key.private_bytes(
        Encoding.PEM,
        PrivateFormat.PKCS8,
        NoEncryption(),
    ).decode()
    thumbprint = cert.fingerprint(hashes.SHA1()).hex().upper()
    return {"private_key": private_key_pem, "thumbprint": thumbprint}


def get_access_token(
    client_id: str,
    tenant_id: str,
    client_secret: Optional[str] = None,
    certificate_path: Optional[str] = None,
    certificate_password: Optional[str] = None,
) -> str:
    """Acquire a Graph token; defaults to local MDMPythonGraph.pfx if present."""
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    default_pfx = Path(__file__).resolve().parent / "MDMPythonGraph.pfx"
    if not certificate_path and default_pfx.exists():
        certificate_path = str(default_pfx)
    credential = None
    cert_error: Optional[Exception] = None
    if certificate_path:
        try:
            credential = _load_pfx_certificate(certificate_path, certificate_password)
        except Exception as exc:  # capture to allow fallback
            cert_error = exc
    if not credential and client_secret:
        credential = client_secret
    if not credential:
        raise cert_error or ValueError("Provide either client_secret or certificate_path to authenticate.")

    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=credential,
    )
    token_response = app.acquire_token_for_client(scopes=GRAPH_SCOPE)
    if "access_token" not in token_response:
        raise RuntimeError(f"Failed to obtain access token: {token_response}")
    return token_response["access_token"]


def send_email(access_token, sender, recipient, subject, body, attachment_path=None):
    endpoint = f"https://graph.microsoft.com/v1.0/users/{sender}/sendMail"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    email_message = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body,
            },
            "toRecipients": [{"emailAddress": {"address": recipient}}],
        },
        "saveToSentItems": True,
    }
    if attachment_path:
        attachment_bytes = Path(attachment_path).expanduser().read_bytes()
        content_bytes = base64.b64encode(attachment_bytes).decode("utf-8")
        email_message["message"]["attachments"] = [
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": Path(attachment_path).name,
                "contentBytes": content_bytes,
            }
        ]
    response = requests.post(endpoint, headers=headers, json=email_message, timeout=30)
    if response.status_code == 202:
        print("Email sent successfully!")
        return
    try:
        details = response.json()
    except ValueError:
        details = response.text
    raise RuntimeError(f"Failed to send email ({response.status_code}): {details}")


if __name__ == "__main__":
    CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "226d7d48-3f88-46dc-b39f-62e1eb11e273")
    TENANT_ID = os.getenv("AZURE_TENANT_ID", "001f9f70-baf4-4a4e-9a03-03c7289f290c")
    CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
    default_pfx = Path(__file__).resolve().parent / "MDMPythonGraph.pfx"
    CERT_PATH = os.getenv("AZURE_CERT_PATH") or (str(default_pfx) if default_pfx.exists() else None)
    CERT_PASSWORD = os.getenv("AZURE_CERT_PASSWORD")  # keep None to allow prompt

    token = get_access_token(
        client_id=CLIENT_ID,
        tenant_id=TENANT_ID,
        client_secret=CLIENT_SECRET,
        certificate_path=CERT_PATH,
        certificate_password=CERT_PASSWORD,
    )
    send_email(
        access_token=token,
        sender=os.getenv("MAIL_SENDER", "mdm.report@snetor.com"),
        recipient=os.getenv("MAIL_RECIPIENT", "k.luce@snetor.com"),
        subject="Test Email",
        body="This is a test email sent using Microsoft Graph API.",
    )
