import json
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

MAIL_BODY = """
    Bonjour,<br>
    Vous trouverez en pièce jointe le rapport recensant la liste des Business Partners ayant potentiellement un numéro de VAT invalide.<br>
    Bonne journée.
    """


def _coerce_id_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ID_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def get_bad_vat(df: pd.DataFrame) -> pd.DataFrame:
    wrong = df[df["Valid"] == "NO"]
    wrong = wrong[wrong["VAT Number"] != "XXXXXXXXXXXXXX"]
    if not wrong.empty:
        with open(
            r"\\snetor-docs\Users\\MDM\998_CHecks\BP-AUTOCHECKS\VAT EXEPTIONS\ignoreVAT.json",
            "r",
            encoding="utf-8",
        ) as f:
            data = json.load(f)
        vats = set(data["vats"])
        vat_full = wrong["MS Code"].astype(str) + wrong["VAT Number"].astype(str)
        wrong = wrong[~vat_full.isin(vats)]
    return wrong


def get_bad_VAT(df: pd.DataFrame) -> pd.DataFrame:
    return get_bad_vat(df)


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
        body=MAIL_BODY,
        file_path=file_path,
        logger=logger,
    )


def main(path: str, mail: bool = True, logger=None):
    debug, log, warn, error = log_helpers(logger)
    vat_path = os.path.join(path, r"vat\report_concatenated.xlsx")
    bad_vat_path = os.path.join(path, r"vat\bad_vats.xlsx")

    vat = pd.read_excel(vat_path, dtype=str)
    vat = _coerce_id_columns(vat)
    bad_vat = get_bad_vat(vat)

    try:
        if not bad_vat.empty:
            _save_df(bad_vat, bad_vat_path, logger=logger)
            if mail:
                send_mail(subject="bad_vats", file_path=bad_vat_path, logger=logger)
        elif mail:
            log("No bad_vats anomalies; sending empty notification.")
            send_mail(subject="bad_vats", logger=logger)
    except Exception as e:
        warn(f"Error while sending bad_vats mail :\n{e}")

    log("VAT mail export finished.")
    
if __name__ == "__main__":
    
    path = r"Z:\MDM\998_CHecks\BP-AUTOCHECKS\ARCHIVES\2026-03-19_03-02_REPORT"
    logger = logger(mail=True, path=__file__, subject="test mails autocheck vat")
    main(path=path, mail=True, logger=logger)
