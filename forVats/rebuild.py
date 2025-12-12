import os
import re

import pandas as pd


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


def rebuild(path: str, infos: pd.DataFrame):
    """
    Ajoute une colonne BP au report VATS, en croisant les VAT du report avec celles de `infos`.
    La clé est `MS Code` + `VAT Number` (normalisés).
    """
    report_path = os.path.join(path, "report_concatenated.xlsx")
    df = pd.read_excel(report_path)

    # Prépare une table VAT -> liste de BP depuis les infos d'origine
    infos = infos.copy()
    infos["VAT_clean"] = infos.get("VAT", "").map(_clean_vat)
    vat_to_bps = (
        infos.groupby("VAT_clean")["BP"]
        .apply(list)
        .to_dict()
    )

    # Construit la clé VAT pour le report et associe les BP
    df["VAT_key"] = (
        df["MS Code"].map(_clean_vat)
        + df["VAT Number"].map(_clean_vat)
    )
    df["BP"] = df["VAT_key"].map(vat_to_bps).apply(lambda x: x if isinstance(x, list) else [])
    df = df.drop(columns=["VAT_key"])

    df.to_excel(report_path, index=False)
    return df
