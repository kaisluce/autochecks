import re
from decimal import Decimal, InvalidOperation

import pandas as pd


def _normalize_identifier(value: str):
    """
    Clean an identifier by keeping digits only (avoids scientific notation artifacts).
    """
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() == "nan" or s == "":
        return None
    candidate = s.replace(",", ".")
    try:
        if "e" in candidate.lower() or "." in candidate:
            candidate = format(Decimal(candidate), "f")
    except InvalidOperation:
        candidate = s
    digits = re.sub(r"\D", "", candidate)
    return digits or None


def main(partner, df: pd.DataFrame):
    """
    Traite les données d'un partenaire pour extraire et valider SIREN/SIRET/VAT.
    """
    siren_series = df.loc[df["BP"] == partner, "siren"].astype(str)
    siret_series = df.loc[df["BP"] == partner, "siret"].astype(str)
    vat_series = df.loc[df["BP"] == partner, "VAT"].astype(str)

    siren = _normalize_identifier(siren_series.iloc[0] if not siren_series.empty else None)
    siret = _normalize_identifier(siret_series.iloc[0] if not siret_series.empty else None)
    vat = vat_series.iloc[0].strip() if not vat_series.empty else None
    if vat is not None and vat.lower() in ("nan", ""):
        vat = None

    doubles_siret = df.loc[(df["siret"] == siret) & (df["BP"] != partner), "BP"].tolist()
    doubles_siren = df.loc[(df["siren"] == siren) & (df["BP"] != partner), "BP"].tolist()

    out = {
        "partner": partner,
        "siren": siren,
        "siret": siret,
        "VAT": vat,
        "duplicates_siren": doubles_siren,
        "duplicates_siret": doubles_siret,
        "missing siren": False,
        "missing siret": False,
        "Missing_Vat": False,
        "Missmatching siren siret" : False,
        "Missmatching siren VAT" : False,
        "uses a snetor siren": False,
        "uses a snetor siret": False,
        "uses a snetor VAT": False,
    }
    
    if siren is None:
        out["missing siren"] = True
    if siret is None:
        out["missing siret"] = True
    if vat is None:
        out["Missing_Vat"] = True

    if siret and siren and siret[:9] != siren:
        out["Missmatching siren siret"] = True
    if vat and len(vat) > 4 and siren and vat[4:] != siren:
        out["Missmatching siren VAT"] = True

    if siren is not None and len(siren) != 9:
        out["siren"] = f"Invalid input ({siren})"
        out["missing siren"] = True
    if siret is not None and len(siret) != 14:
        out["siret"] = f"Invalid input ({siret})"
        out["missing siret"] = True

    if checkSnetor(siren):
        out["uses a snetor siren"] = True
    if checkSnetor(siret):
        out["uses a snetor siret"] = True
    if checkSnetor(vat):
        out["uses a snetor VAT"] = True

    return out


def checkSnetor(input):
    """
    Vérifie si un identifiant appartient à la liste d'identifiants liés à Snetor.
    """
    if input is None:
        return False
    if len(input) == 9:
        return any(input in vat for vat in snetor_VAT)
    if len(input) == 14:
        return any(input[:9] in vat for vat in snetor_VAT)
    return any(input in vat for vat in snetor_VAT)


snetor_VAT = [
    "BE0749997169",
    "NL825733273B01",
    "BE0899097750",
    "DE325236398",
    "SI49978608",
    "BE0782782575",
    "GB134706719",
    "FR25784158545",
    "FR72822870226",
    "FR75383926409",
    "FR36411096290",
    "FR58300960622",
    "BE0476868529",
    "DE322263583",
]
