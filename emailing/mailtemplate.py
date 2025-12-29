import asyncio
import configparser
import sys
from pathlib import Path

from msgraph.generated.models.file_attachment import FileAttachment
from azure.identity import CertificateCredential
from msgraph.graph_service_client import GraphServiceClient
from msgraph.generated.models.body_type import BodyType
from msgraph.generated.models.email_address import EmailAddress
from msgraph.generated.models.item_body import ItemBody
from msgraph.generated.models.message import Message
from msgraph.generated.models.recipient import Recipient
from msgraph.generated.users.item.send_mail.send_mail_post_request_body import (
    SendMailPostRequestBody,
)
import datetime

# Microsoft Graph email helper: loads config/cert and sends reports.
# Charge clientId/tenantId depuis config.cfg (et config.dev.cfg si present)
BASE_DIR = Path(__file__).resolve().parent


def _candidate_paths() -> list[Path]:
    """
    Build a list of possible config locations.

    We check:
    - alongside this module (source checkout or inside the PyInstaller bundle)
    - alongside the executable (PyInstaller onefile)
    - the parent project folder
    - the current working directory
    """
    exe_dir = Path(sys.argv[0]).resolve().parent
    runtime_dir = Path(getattr(sys, "_MEIPASS", BASE_DIR))
    candidates = [
        BASE_DIR / "config.cfg",
        BASE_DIR / "config.dev.cfg",
        BASE_DIR.parent / "config.cfg",
        BASE_DIR.parent / "config.dev.cfg",
        exe_dir / "config.cfg",
        exe_dir / "config.dev.cfg",
        runtime_dir / "config.cfg",
        runtime_dir / "config.dev.cfg",
        Path.cwd() / "config.cfg",
        Path.cwd() / "config.dev.cfg",
    ]
    # Preserve order but drop duplicates
    seen = set()
    unique = []
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        unique.append(path)
    return unique


def _log_helpers(logger=None):
    def _info(msg):
        if logger is None:
            print(f"[MAIL] {msg}")
        elif hasattr(logger, "log"):
            logger.log(msg)
        elif hasattr(logger, "info"):
            logger.info(msg)
        else:
            logger(msg)

    def _warn(msg):
        if logger is None:
            print(f"[MAIL][WARN] {msg}")
        elif hasattr(logger, "warn"):
            logger.warn(msg)
        elif hasattr(logger, "warning"):
            logger.warning(msg)
        else:
            _info(f"[WARN] {msg}")

    def _error(msg):
        if logger is None:
            print(f"[MAIL][ERROR] {msg}")
        elif hasattr(logger, "error"):
            logger.error(msg)
        else:
            _info(f"[ERROR] {msg}")
    return _info, _warn, _error


def _load_config(logger=None) -> configparser.ConfigParser:
    """
    Load azure settings lazily so the application can run even when the config
    is missing (e.g., when emails are disabled).
    """
    config = configparser.ConfigParser(interpolation=None)
    candidates = _candidate_paths()
    read_files = config.read(candidates)
    if "azure" not in config:
        tried = ", ".join(str(p) for p in candidates)
        read = ", ".join(read_files) if read_files else "none"
        raise RuntimeError(f"No [azure] section found. Tried: {tried}. Loaded: {read}.")
    return config

async def main(subject : str | "Global", file_path : str | None = None, logger=None) -> None:
    _info, _warn, _error = _log_helpers(logger)
    config = _load_config(logger=logger)
    azure_settings = config["azure"]
    tenant_id = azure_settings["tenantId"]
    client_id = azure_settings["clientId"]

    # PFX place a la racine du projet par defaut, surcharge possible via config
    cert_path_setting = azure_settings.get("certificatePath", "MDMPythonGraphV2.pfx")
    cert_path = Path(cert_path_setting)
    if not cert_path.is_absolute():
        # Resolve relative paths from the emailing/ directory so the script remains portable
        cert_path = (BASE_DIR / cert_path).resolve()
    cert_password = azure_settings.get("certificatePassword") or None
    if cert_password and cert_password.startswith('"') and cert_password.endswith('"'):
        cert_password = cert_password[1:-1]

    shared_mailbox_upn = "mdm.report@snetor.com"

    credential = CertificateCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        certificate_path=str(cert_path),
        password=cert_password,
    )

    graph = GraphServiceClient(credential, scopes=["https://graph.microsoft.com/.default"])
    _info(f"Prepared Graph client for mailbox send. Attachment={file_path}")
    
    match subject:
        case "closed_siret":
            mail_body = """
            Bonjour,<br>
            Vous trouverez en pièce jointe le rapport recensant la liste des Business Partners présentant potentiellement un siret inactif.<br>
            Bonne journée.
            """
        case "closed_siren":
            mail_body = """
            Bonjour,<br>
            Vous trouverez en pièce jointe le rapport recensant la liste des Business Partners dont la société est potentiellement cessée.<br>
            Bonne journée.
            """
        case "duplicated_siret":
            mail_body = """
            Bonjour,<br>
            Vous trouverez en pièce jointe le rapport recensant la liste des Business Partners présentant potentiellement un siret apparaissant sur plusieurs BP.<br>
            Bonne journée.
            """
        case "wrong_name":
            mail_body = """
            Bonjour,<br>
            Vous trouverez en pièce jointe le rapport recensant la liste des Business Partners présentant potentiellement un nom mal enregistré ou manquant.<br>
            Bonne journée.
            """
        case "bad_vats":
            mail_body = """
            Bonjour,<br>
            Vous trouverez en pièce jointe le rapport recensant la liste des Business Partners ayant potentiellement un numéro de VAT invalide.<br>
            Bonne journée.
            """
        case _:
            mail_body = """
            Bonjour,<br>
            Vous trouverez en pièce jointe le rapport recensant la liste des Business Partners.<br>
            Bonne journée.
            """
    
    message = Message(
        # subject=f"MDM Autochecks report {datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')} ({subject})",
        subject = f"MDM Quality Check Report - [{subject}]",
        body=ItemBody(
            content_type=BodyType.Html,
            content=mail_body,
        ),
        to_recipients=[
            Recipient(email_address=EmailAddress(address="masterdata@snetor.com"))
            # Recipient(email_address=EmailAddress(address="k.luce@snetor.com"))
        ],
    )

    _info(f"filepath: {file_path}")
    if file_path is not None:
        file = Path(file_path)

        # Pass raw bytes; the SDK handles base64 encoding when serializing
        attachment = FileAttachment(
            name=file.name,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            content_bytes=file.read_bytes(),
        )
        
        message.attachments=[attachment]
    else:
        message.body = ItemBody(
            content_type=BodyType.Html,
            content=f"""
            Bonjour,<br>
            Aucune anomalie n'a été trouvée sur les business partners concernant le {subject}.<br>
            Bonne journée.
            """,
        )
        
    request_body = SendMailPostRequestBody(
        message=message,
        save_to_sent_items=True,
    )

    await graph.users.by_user_id(shared_mailbox_upn).send_mail.post(request_body)
    _info("Mail envoyé avec succès.")


async def errormail(error_message: str, subject: str = "Autochecks error", logger=None) -> None:
    """
    Send an error notification email with the provided message (no attachment).
    """
    _info, _warn, _error = _log_helpers(logger)
    config = _load_config(logger=logger)
    azure_settings = config["azure"]
    tenant_id = azure_settings["tenantId"]
    client_id = azure_settings["clientId"]

    cert_path_setting = azure_settings.get("certificatePath", "MDMPythonGraphV2.pfx")
    cert_path = Path(cert_path_setting)
    if not cert_path.is_absolute():
        cert_path = (BASE_DIR / cert_path).resolve()
    cert_password = azure_settings.get("certificatePassword") or None
    if cert_password and cert_password.startswith('"') and cert_password.endswith('"'):
        cert_password = cert_password[1:-1]

    shared_mailbox_upn = "mdm.report@snetor.com"

    credential = CertificateCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        certificate_path=str(cert_path),
        password=cert_password,
    )

    graph = GraphServiceClient(credential, scopes=["https://graph.microsoft.com/.default"])
    _info("Preparing error email")

    message = Message(
        subject=f"[MDM Autochecks] {subject}",
        body=ItemBody(
            content_type=BodyType.Html,
            content=f"<p>An error occurred during the Autochecks run:</p><pre>{error_message}</pre>",
        ),
        to_recipients=[
            Recipient(email_address=EmailAddress(address="masterdata@snetor.com"))
        ],
    )

    request_body = SendMailPostRequestBody(
        message=message,
        save_to_sent_items=True,
    )

    await graph.users.by_user_id(shared_mailbox_upn).send_mail.post(request_body)
    _info("Error email sent successfully.")


if __name__ == "__main__":
    asyncio.run(main("closed_siret", r"Z:\MDM\998_CHecks\2025-12-17_15-24_REPORT\siren_siret\closed_siret.xlsx"))
