import os
import sys
from pathlib import Path

import pandas as pd

# Ensure project root is on sys.path so imports work whether run as module or script.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mails import send_quality_check_mail
from logger import logger, log_helpers


ID_COLUMNS = ("BP", "Business Partner", "siren", "siret")

MAIL_BODY_CLOSED_SIRET = """
    Bonjour,<br>
    Vous trouverez en pièce jointe le rapport recensant la liste des Business Partners présentant potentiellement un siret inactif.<br>
    Bonne journée.
    """

MAIL_BODY_CLOSED_SIREN = """
    Bonjour,<br>
    Vous trouverez en pièce jointe le rapport recensant la liste des Business Partners dont la société est potentiellement cessée.<br>
    Bonne journée.
    """

MAIL_BODY_DUPLICATED_SIRET = """
    Bonjour,<br>
    Vous trouverez en pièce jointe le rapport recensant la liste des Business Partners présentant potentiellement un siret apparaissant sur plusieurs BP.<br>
    Bonne journée.
    """

MAIL_BODIES = {
    "closed_siret": MAIL_BODY_CLOSED_SIRET,
    "closed_siren": MAIL_BODY_CLOSED_SIREN,
    "duplicated_siret": MAIL_BODY_DUPLICATED_SIRET,
}


def _coerce_id_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ID_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def get_closed_siret(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["status"].isin(["Ferm\u00e9", "FermÃ©", "FermÃƒÂ©"])]


def get_stopped_siren(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["status"].isin(["Cess\u00e9e", "CessÃ©e", "CessÃƒÂ©e"])]


def get_duplicated_siret(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["duplicates_siret"] != "[]"]


def _save_df(df: pd.DataFrame, path: str, logger=None):
    debug, log, warn, error = log_helpers(logger)
    with pd.ExcelWriter(path, engine="xlsxwriter", mode="w") as writer:
        df.to_excel(writer, index=False, sheet_name="Report", header=True)
    log(f"Saved {len(df)} rows to {path}")


def send_mail(subject: str, file_path: str | None = None, logger=None) -> None:
    debug, log, warn, error = log_helpers(logger)
    if file_path is None:
        log(f"Sending email without attachment for subject={subject}")
    else:
        log(f"Sending email with attachment: {file_path}")
    send_quality_check_mail(
        subject=subject,
        body=MAIL_BODIES.get(subject, ""),
        file_path=file_path,
        logger=logger,
    )


def _dispatch_report(df: pd.DataFrame, output_path: str, subject: str, mail: bool, logger=None):
    debug, log, warn, error = log_helpers(logger)
    try:
        if not df.empty:
            _save_df(df, output_path, logger=logger)
            if mail:
                send_mail(subject=subject, file_path=output_path, logger=logger)
        elif mail:
            log(f"No {subject} anomalies; sending empty notification.")
            send_mail(subject=subject, logger=logger)
    except Exception as e:
        warn(f"Error while sending {subject} mail :\n{e}")


def main(path: str, mail: bool = True, logger=None):
    debug, log, warn, error = log_helpers(logger)
    log("Starting the mail process for the Siren/Siret job")
    siren_path = os.path.join(path, r"siren_siret\latest_report.xlsx")
    datas_path = os.path.join(path, r"latest_datas.xlsx")

    df1 = pd.read_excel(siren_path, dtype=str)
    datas = pd.read_excel(datas_path, dtype=str)
    df1 = _coerce_id_columns(df1)
    datas = _coerce_id_columns(datas)

    closed_path = os.path.join(path, r"siren_siret\closed_siret.xlsx")
    stopped_path = os.path.join(path, r"siren_siret\closed_siren.xlsx")
    dupe_path = os.path.join(path, r"siren_siret\duplicated_siret.xlsx")

    closed = get_closed_siret(df1)
    stopped = get_stopped_siren(df1)
    dupe = get_duplicated_siret(df1)

    _dispatch_report(closed, closed_path, "closed_siret", mail=mail, logger=logger)
    _dispatch_report(stopped, stopped_path, "closed_siren", mail=mail, logger=logger)
    _dispatch_report(dupe, dupe_path, "duplicated_siret", mail=mail, logger=logger)

    log("SIREN/SIRET mail export finished.")

if __name__ == "__main__":
    
    path = r"Z:\MDM\998_CHecks\BP-AUTOCHECKS\ARCHIVES\2026-03-19_03-02_REPORT"
    logger = logger(mail=True, path=__file__, subject="test mails autocheck siren")
    main(path=path, mail=True, logger=logger)