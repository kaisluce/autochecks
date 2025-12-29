import time
from typing import Any, Dict

import requests as rq
from requests.exceptions import RequestException


# Thin wrapper around the INSEE SIRENE API for SIREN lookups with retry logic.
def _get_with_retry(url: str, delay: float = 3.0, max_duration: float = 600.0) -> rq.Response:
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
            resp = rq.get(url, timeout=10)
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
