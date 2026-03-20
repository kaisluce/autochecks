import time
from typing import Any, Dict

import requests as rq
from requests.exceptions import RequestException


# Thin wrapper around the INSEE SIRENE API for SIREN lookups with retry logic.
def _get_with_retry(url: str, delay: float = 3.0, max_duration: float = 600.0, headers: dict = None) -> rq.Response:
    """
    Retry GET until success or max_duration (seconds) is reached.
    delay: seconds between attempts (default 3s)
    max_duration: total retry window (default 10 minutes)
    """
    start = time.time()
    attempt = 0
    last_exc = None
    while time.time() - start < max_duration:
        attempt += 1
        try:
            resp = rq.get(url, timeout=10, headers=headers)
            if resp.status_code >= 500:
                print(f"[WARN] Retry {attempt}: HTTP {resp.status_code} for {url} (server error)")
                time.sleep(delay)
                continue
            return resp
        except RequestException as exc:
            last_exc = exc
            print(f"[WARN] Retry {attempt}: network error {exc} for {url}")
            time.sleep(delay)
    if last_exc:
        raise RuntimeError(f"Échec réseau après {attempt} tentatives (~{max_duration}s) : {last_exc}") from last_exc
    raise RuntimeError(f"Impossible de joindre l'API après ~{max_duration}s")


def handlesiren(siren: str) -> Dict[str, Any]:
    """
    Interroge l'API SIRENE de l'INSEE pour obtenir les informations d'une unité légale via son numéro SIREN.

    Args:
        siren (str): Le numéro SIREN à 9 chiffres à rechercher.

    Returns:
        dict: Un dictionnaire contenant les informations formatées de l'unité légale si la requête
              réussit. Les champs incluent la dénomination, le statut, la date de création,
              l'adresse du siège, etc.
              En cas d'échec de la requête (par exemple, statut 404 pour un SIREN non trouvé),
              renvoie un dictionnaire avec une clé 'error'.
    """
    url = f"https://api-avis-situation-sirene.insee.fr/identification/siren/{siren}?telephone="
    response = _get_with_retry(url)
    # Vérifie si la requête HTTP a réussi (par exemple, code de statut 200 OK).
    if response.status_code == 200:
        data = response.json()
        unite_legale = data["uniteLegale"]["periodesUniteLegale"][0]
        hq = data["etablissements"][0]
        out = {
            "siren" : data["uniteLegale"]["siren"],
            "denomination" : unite_legale["denominationUniteLegale"],
            "status" : unite_legale["libelleEtatAdministratifUniteLegale"] or "invalid input",
            "date_creation" : data["uniteLegale"]["dateCreationUniteLegale"],
            "naf" : unite_legale["activitePrincipaleUniteLegale"],
            "naf_label" : unite_legale["libelleActivitePrincipaleUniteLegale"],
            "cat_juridique" : unite_legale["libelleCategorieJuridiqueUniteLegale"],
            "adresse" : f"{hq['adresseEtablissement']['numeroVoieEtablissement']} "
                f"{hq['adresseEtablissement']['typeVoieEtablissement']} "
                f"{hq['adresseEtablissement']['libelleVoieEtablissement']}, "
                f"{hq['adresseEtablissement']['codePostalEtablissement']} "
                f"{hq['adresseEtablissement']['libelleCommuneEtablissement']}",
            "n_voie" : hq["adresseEtablissement"]['numeroVoieEtablissement'],
            "voie" : hq["adresseEtablissement"]["libelleVoieEtablissement"],
            "code_postal" : hq["adresseEtablissement"]["codePostalEtablissement"],
            "commune" :hq["adresseEtablissement"]["libelleCommuneEtablissement"],
            "siret_siege" : hq["siret"],
        }
        # Si l'unité légale est cessée ('C'), ajoute la date de cessation.
        if unite_legale["etatAdministratifUniteLegale"] == "C":
            out["date_cessation"] = unite_legale["dateDebut"]
        
        return out
    else:
        return {"error": f"Failed to retrieve data for SIREN {siren}, status code: {response.status_code}"}

_INFOGREFFE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0",
    "Origin": "https://www.infogreffe.fr",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "fr,fr-FR;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Ocp-Apim-Subscription-Key": "7f50925c359346ecb8913668e2637bd3",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

def fallback_infogreffe(siren: str) -> Dict[str, Any]:
    url = f"https://www.api.infogreffe.fr/athena/recherche-api/recherche/entreprises_etablissements?numero_identification={siren}&limit=10&offset=0"
    response = _get_with_retry(url, headers=_INFOGREFFE_HEADERS)
    if response.status_code == 200:
        data = response.json()
        if not data.get("data"):
            return {"error": f"No data found for SIREN {siren} on infogreffe"}
        item = data["data"][0]
        adresse = item.get("adresse", {})
        declaree = adresse.get("adresse_declaree", {})
        naf = item.get("activite_naf", {})

        ligne1 = declaree.get("ligne1") or ""
        code_postal = declaree.get("code_postal") or ""
        commune = declaree.get("bureau_distributeur") or ""
        adresse_str = f"{ligne1}, {code_postal} {commune}".strip(", ")

        out = {
            "siren": item.get("numero_identification"),
            "denomination": item.get("nom_entreprise"),
            "status": item.get("etat") or "unknown",
            "naf": naf.get("code"),
            "naf_label": naf.get("libelle"),
            "adresse": adresse_str,
            "n_voie": None,
            "voie": ligne1,
            "code_postal": code_postal,
            "commune": commune,
            "siret_siege": f"{item.get('numero_identification')}{str(item.get('nic', '')).zfill(5)}" if item.get("nic") else None,
        }

        if item.get("etat") == "RADIEE":
            out["date_cessation"] = item.get("date_radiation")

        return out
    else:
        return {"error": f"Failed to retrieve data for SIREN {siren} on infogreffe, status code: {response.status_code}"}

if __name__ == "__main__":
    _SOCIETE_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0",
        "Accept": "*/*",
        "Accept-Language": "fr,fr-FR;q=0.9,en;q=0.8",
        "Referer": "https://www.societe.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }

    import re

    _SOCIETE_HEADERS["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    _SOCIETE_HEADERS["Sec-Fetch-Mode"] = "navigate"
    _SOCIETE_HEADERS["Sec-Fetch-Site"] = "none"
    _SOCIETE_HEADERS["Sec-Fetch-Dest"] = "document"

    for siren in ["780095295", "552081317"]:
        url = f"https://www.societe.com/societe/fiche-{siren}.html"
        resp = rq.get(url, headers=_SOCIETE_HEADERS, timeout=10)
        print(f"\n--- {siren} | {resp.status_code} ---")
        if resp.status_code == 200:
            html = resp.text
            for keyword in ["radiée", "Active", "cessée", "en activité", "fermée", "dissout"]:
                matches = [(m.start(), html[max(0,m.start()-60):m.end()+60]) for m in re.finditer(keyword, html, re.IGNORECASE)]
                if matches:
                    print(f"  [{keyword}] trouvé {len(matches)}x — ex: ...{matches[0][1]}...")
        else:
            print(resp.text[:200])
