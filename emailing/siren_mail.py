import asyncio
import os
import sys
from pathlib import Path

import pandas as pd

# Ensure project root is on sys.path so imports work whether run as module or script.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fetchNames.get_names_from_last_report as get_names

if __name__ == "__main__":
    import mailtemplate as mailtemplate
else:
    import emailing.mailtemplate as mailtemplate


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

    return _info, _warn


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
    _info, _warn = _logger_helpers(logger)
    with pd.ExcelWriter(path, engine="xlsxwriter", mode="w") as writer:
        df.to_excel(writer, index=False, sheet_name="Report", header=True)
    _info(f"Saved {len(df)} rows to {path}")


def _send_with_file(file_path: str, logger=None) -> None:
    _info, _warn = _logger_helpers(logger)
    subject = Path(file_path).stem
    _info(f"Sending email with attachment: {file_path}")
    asyncio.run(mailtemplate.main(subject, file_path, logger=logger))


def _send(subject: str, logger=None) -> None:
    _info, _warn = _logger_helpers(logger)
    _info(f"Sending email without attachment for subject={subject}")
    asyncio.run(mailtemplate.main(subject, logger=logger))


def _dispatch_report(df: pd.DataFrame, output_path: str, empty_subject: str, mail: bool, logger=None):
    _info, _warn = _logger_helpers(logger)
    try:
        if not df.empty:
            _save_df(df, output_path, logger=logger)
            if mail:
                _send_with_file(output_path, logger=logger)
        elif mail:
            _info(f"No {empty_subject} anomalies; sending empty notification.")
            _send(empty_subject, logger=logger)
    except Exception as e:
        _warn(f"Error while sending {empty_subject} mail :\n{e}")


def main(path: str, mail: bool = True, logger=None):
    _info, _warn = _logger_helpers(logger)
    _info("Starting the mail process for the Siren/Siret job")
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

    _info("SIREN/SIRET mail export finished.")
