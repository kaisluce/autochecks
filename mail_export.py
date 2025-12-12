import base64
import os
from pathlib import Path
from typing import Optional

import msal
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

DIRECTORY_LOCATION = os.path.join(os.getenv("DIRECTORY_LOCATION", ""), "2025-12-10_12-03_REPORT")


def _load_pfx_certificate(pfx_path: str, password: str):
    """Load PFX and return dict usable by MSAL (private key PEM + thumbprint)."""
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat, pkcs12
    except ImportError as exc:
        raise ImportError("Install 'cryptography' to use certificate-based auth.") from exc

    pfx_bytes = Path(pfx_path).expanduser().read_bytes()
    private_key, cert, _ = pkcs12.load_key_and_certificates(
        pfx_bytes,
        password.encode() if password else None,
    )
    if not private_key or not cert:
        raise ValueError("PFX file does not contain a private key and certificate.")

    private_key_pem = private_key.private_bytes(
        Encoding.PEM,
        PrivateFormat.PKCS8,
        NoEncryption(),
    ).decode()
    thumbprint = cert.fingerprint(hashes.SHA1()).hex().upper()
    return {"private_key": private_key_pem, "thumbprint": thumbprint}


def get_closed_siret(df: pd.DataFrame):
    closed = df[df["status"] == "Fermé"]
    return closed

def get_stopped_siren(df: pd.DataFrame):
    stopped = df[df["status"] == "Cessée"]
    return stopped

def get_duplicated_siret(df: pd.DataFrame):
    dupe = df[df["duplicates_siret"]!="[]"]
    return dupe

def get_wrong_name(df: pd.DataFrame):
    wrong = df[df["diagnostic_name"]!="exact"]
    return wrong

def get_bad_VAT(df: pd.DataFrame):
    wrong = df[df["Valid"]=="NO"]
    return wrong

def save_df(df : pd.DataFrame, path : str):
    with pd.ExcelWriter(path, engine="xlsxwriter", mode = "w") as writer:
        # Crée une feuille vide avec les en-têtes de colonnes corrects.
        df.to_excel(
            writer, index=False, sheet_name="Report", header=True
        )


def main(path : str):
    df1_path = os.path.join(path, "siren_siret/latest_report.xlsx")
    vat_path = os.path.join(path, "vat/report_concatenated.xlsx")
    df1 = pd.read_excel(df1_path)
    vat = pd.read_excel(vat_path)
    closed_path = os.path.join(path, "siren_siret/closed_siret.xlsx")
    stopped_path = os.path.join(path, "siren_siret/stopped_siren.xlsx")
    dupe_path = os.path.join(path, "siren_siret/duplicated_siret.xlsx")
    wrong_path = os.path.join(path, "siren_siret/wrong_name.xlsx")
    bad_vat_path = os.path.join(path, "vat/bad_vats.xlsx")
    closed = get_closed_siret(df1)
    stopped = get_stopped_siren(df1)
    dupe = get_duplicated_siret(df1)
    wrong = get_wrong_name(df1)
    bad_vat = get_bad_VAT(vat)

    save_df(closed, closed_path)
    save_df(stopped, stopped_path)
    save_df(dupe, dupe_path)
    save_df(wrong, wrong_path)
    save_df(bad_vat, bad_vat_path)

def get_access_token(
    client_id: str,
    tenant_id: str,
    client_secret: Optional[str] = None,
    certificate_path: Optional[str] = None,
    certificate_password: Optional[str] = None,
) -> str:
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    if not certificate_path and Path("MDMPythonGraph.pfx").exists():
        certificate_path = str(Path("MDMPythonGraph.pfx").resolve())
    if certificate_path:
        credential = _load_pfx_certificate(certificate_path, certificate_password or "")
    elif client_secret:
        credential = client_secret
    else:
        raise ValueError("Provide either client_secret or certificate_path to authenticate.")

    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=credential,
    )
    token_response = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
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
    else:
        try:
            details = response.json()
        except ValueError:
            details = response.text
        raise RuntimeError(f"Failed to send email ({response.status_code}): {details}")


if __name__ == "__main__":
    path = DIRECTORY_LOCATION
    df1_path = os.path.join(path, "siren_siret/latest_report.xlsx")
    vat_path = os.path.join(path, "vat/report_concatenated.xlsx")
    df1 = pd.read_excel(df1_path)
    vat = pd.read_excel(vat_path)
    closed_path = os.path.join(path, "siren_siret/closed_siret.xlsx")
    stopped_path = os.path.join(path, "siren_siret/stopped_siren.xlsx")
    dupe_path = os.path.join(path, "siren_siret/duplicated_siret.xlsx")
    wrong_path = os.path.join(path, "siren_siret/wrong_name.xlsx")
    bad_vat_path = os.path.join(path, "vat/bad_vats.xlsx")
    closed = get_closed_siret(df1)
    stopped = get_stopped_siren(df1)
    dupe = get_duplicated_siret(df1)
    wrong = get_wrong_name(df1)
    bad_vat = get_bad_VAT(vat)

    save_df(closed, closed_path)
    save_df(stopped, stopped_path)
    save_df(dupe, dupe_path)
    save_df(wrong, wrong_path)
    save_df(bad_vat, bad_vat_path)
