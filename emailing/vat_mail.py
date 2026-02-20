import asyncio
import json
import os
import sys
from pathlib import Path

import pandas as pd

# Ensure project root is on sys.path so imports work whether run as module or script.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

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


def main(path: str, mail: bool = True, logger=None):
    _info, _warn = _logger_helpers(logger)
    vat_path = os.path.join(path, r"vat\report_concatenated.xlsx")
    bad_vat_path = os.path.join(path, r"vat\bad_vats.xlsx")

    vat = pd.read_excel(vat_path, dtype=str)
    vat = _coerce_id_columns(vat)
    bad_vat = get_bad_vat(vat)

    try:
        if not bad_vat.empty:
            _save_df(bad_vat, bad_vat_path, logger=logger)
            if mail:
                _send_with_file(bad_vat_path, logger=logger)
        elif mail:
            _info("No bad_vats anomalies; sending empty notification.")
            _send("bad_vats", logger=logger)
    except Exception as e:
        _warn(f"Error while sending bad_vats mail :\n{e}")

    _info("VAT mail export finished.")
