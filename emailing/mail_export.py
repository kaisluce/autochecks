import os
import sys
from pathlib import Path
import asyncio

import pandas as pd
from dotenv import load_dotenv

# Ensure project root is on sys.path so imports work whether run as module or script.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fetchNames.get_names_from_last_report as get_names

if __name__ == "__main__":
    import mailtemplate as mailtemplate
else:
    import emailing.mailtemplate as mailtemplate

load_dotenv()

# Helpers to filter anomaly subsets and email/save the corresponding reports.
DIRECTORY_LOCATION = os.path.join(os.getenv("DIRECTORY_LOCATION", ""), "2025-12-10_12-03_REPORT")

ID_COLUMNS = ("BP", "Business Partner", "siren", "siret")


def _logger_helpers(logger=None):
    def _info(msg: str):
        if logger is None:
            print(f"[MAIL] {msg}")
        elif hasattr(logger, "log"):
            logger.log(msg)
        elif hasattr(logger, "info"):
            logger.info(msg)
        else:
            logger(msg)

    def _warn(msg: str):
        if logger is None:
            print(f"[MAIL][WARN] {msg}")
        elif hasattr(logger, "warn"):
            logger.warn(msg)
        elif hasattr(logger, "warning"):
            logger.warning(msg)
        else:
            _info(f"[WARN] {msg}")

    def _debug(msg: str):
        if logger is None:
            print(f"[MAIL][DEBUG] {msg}")
        elif hasattr(logger, "debug"):
            logger.debug(msg)
        else:
            _info(f"[DEBUG] {msg}")

    def _error(msg: str, exc=None):
        if logger is None:
            print(f"[MAIL][ERROR] {msg}")
            if exc:
                print(exc)
            return
        if hasattr(logger, "error"):
            logger.error(msg, exc_info=bool(exc))
        else:
            _info(f"[ERROR] {msg}")

    return _info, _warn, _debug, _error


def _coerce_id_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ID_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def get_closed_siret(df: pd.DataFrame):
    return df[df["status"] == "Fermé"]


def get_stopped_siren(df: pd.DataFrame):
    return df[df["status"] == "Cessée"]


def get_duplicated_siret(df: pd.DataFrame):
    return df[df["duplicates_siret"] != "[]"]


def get_wrong_name(df: pd.DataFrame):
    return df[~(df["name match diag"].isin(["exact", "Name not fetched", "Missing name"]))]


def get_bad_VAT(df: pd.DataFrame):
    wrong = df[df["Valid"] == "NO"]
    wrong = wrong[wrong["VAT Number"] != "XXXXXXXXXXXXXX"]
    return wrong


def save_df(df: pd.DataFrame, path: str, logger=None):
    _info, _warn, _debug, _error = _logger_helpers(logger)
    with pd.ExcelWriter(path, engine="xlsxwriter", mode="w") as writer:
        df.to_excel(writer, index=False, sheet_name="Report", header=True)
    _info(f"Saved {len(df)} rows to {path}")


def send_with_file(file_path: str, logger=None) -> None:
    subject = Path(file_path).stem
    _info, _warn, _debug, _error = _logger_helpers(logger)
    _info(f"Sending email with attachment: {file_path}")
    asyncio.run(mailtemplate.main(subject, file_path, logger=logger))


def send(subject: str, logger=None) -> None:
    _info, _warn, _debug, _error = _logger_helpers(logger)
    _info(f"Sending email without attachment for subject={subject}")
    asyncio.run(mailtemplate.main(subject, logger=logger))


def main(path: str, mail: bool = True, logger=None):
    _info, _warn, _debug, _error = _logger_helpers(logger)
    df1_path = os.path.join(path, "siren_siret/latest_report.xlsx")
    vat_path = os.path.join(path, "vat/report_concatenated.xlsx")
    datas_path = os.path.join(path, "latest_datas.xlsx")
    _debug(f"Loading reports: {df1_path}, {vat_path}")
    df1 = pd.read_excel(df1_path, dtype=str)
    vat = pd.read_excel(vat_path, dtype=str)
    datas = pd.read_excel(datas_path, dtype=str)
    df1 = _coerce_id_columns(df1)
    vat = _coerce_id_columns(vat)
    datas = _coerce_id_columns(datas)
    closed_path = os.path.join(path, "siren_siret/closed_siret.xlsx")
    stopped_path = os.path.join(path, "siren_siret/closed_siren.xlsx")
    dupe_path = os.path.join(path, "siren_siret/duplicated_siret.xlsx")
    wrong_path = os.path.join(path, "wrong_name.xlsx")
    bad_vat_path = os.path.join(path, "vat/bad_vats.xlsx")
    fetchedNames_path = os.path.join(path, "fetchedNames.xlsx")
    closed = get_closed_siret(df1)
    stopped = get_stopped_siren(df1)
    dupe = get_duplicated_siret(df1)
    bad_vat = get_bad_VAT(vat)
    fetchedNames = get_names.main(vat, datas, df1, logger)
    save_df(fetchedNames, fetchedNames_path, logger=logger)
    wrong = get_wrong_name(fetchedNames)


    if not closed.empty:
        save_df(closed, closed_path, logger=logger)
        if mail:
            send_with_file(closed_path, logger=logger)
    elif mail:
        _info("No closed_siret anomalies; sending empty notification.")
        send("closed_siret", logger=logger)

    if not stopped.empty:
        save_df(stopped, stopped_path, logger=logger)
        if mail:
            send_with_file(stopped_path, logger=logger)
    elif mail:
        _info("No closed_siren anomalies; sending empty notification.")
        send("closed_siren", logger=logger)

    if not dupe.empty:
        save_df(dupe, dupe_path, logger=logger)
        if mail:
            send_with_file(dupe_path, logger=logger)
    elif mail:
        _info("No duplicated_siret anomalies; sending empty notification.")
        send("duplicated_siret", logger=logger)

    if not wrong.empty:
        save_df(wrong, wrong_path, logger=logger)
        if mail:
            send_with_file(wrong_path, logger=logger)
    elif mail:
        _info("No wrong_name anomalies; sending empty notification.")
        send("wrong_name", logger=logger)

    if not bad_vat.empty:
        save_df(bad_vat, bad_vat_path, logger=logger)
        if mail:
            send_with_file(bad_vat_path, logger=logger)
    elif mail:
        _info("No bad_vats anomalies; sending empty notification.")
        send("bad_vats", logger=logger)


if __name__ == "__main__":
    import logger
    path = r"Z:\MDM\998_CHecks\2026-01-05_03-02_REPORT"
    logger = logger.logger(mail=False)
    main(path, mail=False, logger=logger)
