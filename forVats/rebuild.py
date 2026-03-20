import os
import re

import pandas as pd

from logger import log_helpers


def _clean_vat(value) -> str:
    """
    Normalize VAT-like strings: strip spaces, remove non-alphanum, drop trailing .0.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).strip()
    if s.lower() == "nan":
        return ""
    s = re.sub(r"[^A-Za-z0-9]", "", s)
    if s.endswith(".0"):
        s = s[:-2]
    return s.upper()


def rebuild(path: str, infos: pd.DataFrame, logger=None):
    """
    Ajoute une colonne BP au report VATS, en croisant les VAT du report avec celles de `infos`.
    La clé est `MS Code` + `VAT Number` (normalisés).
    """
    _debug, _log, _warn, _error = log_helpers(logger)
    report_path = os.path.join(path, r"report_concatenated.xlsx")
    df = pd.read_excel(report_path, dtype=str)

    # Prépare une table VAT -> liste de BP depuis les infos d'origine
    infos = infos.copy()
    infos["VAT_clean"] = infos.get("VAT", "").map(_clean_vat)
    vat_to_bps = (
        infos.groupby("VAT_clean")["BP"]
        .apply(list)
        .to_dict()
    )
    name_col = "Name 1"
    vat_to_names = {}
    if name_col in infos.columns:
        vat_to_names = (
            infos.groupby("VAT_clean")[name_col]
            .apply(list)
            .to_dict()
        )

    # Construit la clé VAT pour le report et associe les BP
    df["VAT_key"] = (
        df["MS Code"].map(_clean_vat)
        + df["VAT Number"].map(_clean_vat)
    )
    df["BP"] = df["VAT_key"].map(vat_to_bps).apply(lambda x: x if isinstance(x, list) else [])
    if vat_to_names:
        df["Name 1"] = df["VAT_key"].map(vat_to_names).apply(lambda x: x if isinstance(x, list) else [])
    df = df.drop(columns=["VAT_key"])

    df.to_excel(report_path, index=False)
    _log(f"Rebuilt VAT report with BP column: {report_path}")
    return df


if __name__ == "__main__":
    report_dir = r"Z:\MDM\998_CHecks\AUTOCHECKS\2026-01-14_10-01_REPORT\vat"
    datas_path = r"Z:\MDM\998_CHecks\AUTOCHECKS\2026-01-14_10-01_REPORT\latest_datas.xlsx"
    infos_df = pd.read_excel(datas_path).astype(str)
    rebuild(report_dir, infos_df)
