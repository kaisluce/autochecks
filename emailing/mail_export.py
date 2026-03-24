import os
import sys
import asyncio
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Ensure project root is on sys.path so imports work whether run as module or script.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fetchNames.get_names_from_last_report as get_names

from mails import send_quality_check_mail
from logger import logger, log_helpers


load_dotenv()

DIRECTORY_LOCATION = os.path.join(os.getenv("DIRECTORY_LOCATION", ""), "2025-12-10_12-03_REPORT")

ID_COLUMNS = ("BP", "Business Partner", "siren", "siret")


MAIL_BODY = """
    Vous trouverez en pièce jointe le rapport recensant la liste des Business Partners présentant potentiellement un nom mal enregistré ou manquant.
    """


def _coerce_id_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ID_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def get_wrong_name(df: pd.DataFrame) -> pd.DataFrame:
    return df[~(df["name match diag"].isin(["exact", "Name not fetched", "Missing name"]))]


def _save_df(df: pd.DataFrame, path: str, logger=None):
    debug, log, warn, error = log_helpers(logger)
    with pd.ExcelWriter(path, engine="xlsxwriter", mode="w") as writer:
        df.to_excel(writer, index=False, sheet_name="Report", header=True)
    log(f"Saved {len(df)} rows to {path}")


def send_mail(subject: str, file_path: str |None = None, logger=None) -> None:
    debug, log, warn, error = log_helpers(logger)
    if file_path == None:
        log(f"Sending email without attachment for subject={subject}")
    else:
        log(f"Sending email with attachment: {file_path}")
    send_quality_check_mail(
        subject=subject,
        body=MAIL_BODY,
        file_path=file_path,
        logger=logger
        )


def main(path: str, mail: bool = True, logger=None):
    debug, log, warn, error = log_helpers(logger)
    log("Starting names mail export")

    siren_path = os.path.join(path, r"siren_siret\latest_report.xlsx")
    vat_path = os.path.join(path, r"vat\report_concatenated.xlsx")
    datas_path = os.path.join(path, r"latest_datas.xlsx")
    fetched_names_path = os.path.join(path, r"fetchedNames.xlsx")
    wrong_path = os.path.join(path, r"wrong_name.xlsx")

    df1 = pd.read_excel(siren_path, dtype=str)
    vat = pd.read_excel(vat_path, dtype=str)
    datas = pd.read_excel(datas_path, dtype=str)
    df1 = _coerce_id_columns(df1)
    vat = _coerce_id_columns(vat)
    datas = _coerce_id_columns(datas)

    fetched_names = get_names.main(vat, datas, df1, logger)
    _save_df(fetched_names, fetched_names_path, logger=logger)
    wrong = get_wrong_name(fetched_names)

    try:
        if not wrong.empty:
            _save_df(wrong, wrong_path, logger=logger)
            if mail:
                send_mail(subject="wrong_name", file_path=wrong_path, logger=logger)
        elif mail:
            log("No wrong_name anomalies; sending empty notification.")
            send_mail(subject="wrong_name", logger=logger)
    except Exception as e:
        warn(f"Error while sending wrong_name mail :\n{e}")

    log("Names mail export finished.")


if __name__ == "__main__":

    path = r"Z:\MDM\998_CHecks\BP-AUTOCHECKS\ARCHIVES\2026-03-19_03-02_REPORT"
    logger = logger(mail=True, path=__file__, subject="test mails autocheck names")
    main(path=path, mail=True, logger=logger)
