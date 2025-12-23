import asyncio
import configparser
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

# Charge clientId/tenantId depuis config.cfg (et config.dev.cfg si present)
BASE_DIR = Path(__file__).resolve().parent
CONFIG_CANDIDATES = [
    BASE_DIR / "config.cfg",
    BASE_DIR / "config.dev.cfg",
    BASE_DIR.parent / "config.cfg",
    BASE_DIR.parent / "config.dev.cfg",
]

config = configparser.ConfigParser(interpolation=None)
read_files = config.read(CONFIG_CANDIDATES)
if "azure" not in config:
    raise RuntimeError(f"No [azure] section found in {CONFIG_CANDIDATES}")

async def main(subject : str | "Global", file_path : str | None = None) -> None:
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

    print(f"filepath: {file_path}")
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
    print("Mail envoyé avec succès.")


if __name__ == "__main__":
    asyncio.run(main("closed_siret", r"Z:\MDM\998_CHecks\2025-12-17_15-24_REPORT\siren_siret\closed_siret.xlsx"))
