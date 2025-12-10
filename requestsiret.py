import time
from typing import Any, Dict

import requests as rq
from requests.exceptions import RequestException


def _get_with_retry(url: str, retries: int = 3, delay: float = 1.0) -> rq.Response:
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            resp = rq.get(url, timeout=10)
            # Retry on transient 5xx
            if resp.status_code >= 500:
                time.sleep(delay)
                continue
            return resp
        except RequestException as exc:
            last_exc = exc
            time.sleep(delay)
    if last_exc:
        raise RuntimeError(f"Échec réseau après {retries} tentatives : {last_exc}") from last_exc
    raise RuntimeError(f"Impossible de joindre l'API après {retries} tentatives")


def handlesiret(siret: str) -> Dict[str, Any]:
    """
    Interroge l'API SIRENE de l'INSEE pour obtenir les informations d'un établissement via son numéro SIRET.

    Args:
        siret (str): Le numéro SIRET à 14 chiffres à rechercher.

    Returns:
        dict: Un dictionnaire contenant les informations formatées de l'établissement si la requête
              réussit. Les champs incluent le SIREN, la dénomination, le statut, l'adresse, etc.
              En cas d'échec de la requête (par exemple, statut 404 pour un SIRET non trouvé),
              renvoie un dictionnaire avec une clé 'error'.
    """
    url = f"https://api-avis-situation-sirene.insee.fr/identification/siret/{siret}?telephone="
    response = _get_with_retry(url)

    # Vérifie si la requête HTTP a réussi (par exemple, code de statut 200 OK).
    if response.status_code == 200:
        data = response.json()

        # établissement principal (ici unique car tu interroges un seul SIRET)
        etab = data.get('etablissements')[-1]
        period = etab["periodesEtablissement"][0]

        out = {
            "siret": etab['siret'],
            "siren": etab['siren'],
            "denomination": data.get('uniteLegale').get('periodesUniteLegale')[0].get('denominationUniteLegale'),
            "status": period.get("libelleEtatAdministratifEtablissement"),
            "nic": etab["nic"],
            "date_creation" : etab.get("dateCreationEtablissement"),
            "naf": period.get("activitePrincipaleEtablissement"),
            "naf_label": period.get("libelleActivitePrincipaleEtablissement"),
            "adresse" : f"{etab['adresseEtablissement']['numeroVoieEtablissement']} "
                f"{etab['adresseEtablissement']['typeVoieEtablissement']} "
                f"{etab['adresseEtablissement']['libelleVoieEtablissement']}, "
                f"{etab['adresseEtablissement']['codePostalEtablissement']} "
                f"{etab['adresseEtablissement']['libelleCommuneEtablissement']}",
            "n_voie": etab["adresseEtablissement"]['numeroVoieEtablissement'],
            "voie": etab["adresseEtablissement"]["libelleVoieEtablissement"],
            "code_postal": etab["adresseEtablissement"]["codePostalEtablissement"],
            "commune": etab["adresseEtablissement"]["libelleCommuneEtablissement"],
            "siege" : etab.get('etablissementSiege'),
        }
        # Si l'établissement n'est pas le siège, trouve le SIRET du siège dans la liste des établissements.
        if not out.get('siege'):
            out['siret_siege'] = data['etablissements'][0]['siret']
        else:
            out['siret_sege'] = siret

        # Si l'établissement est fermé ('F'), ajoute la date de cessation.
        if period.get("etatAdministratifEtablissement") == "F":
            out["date_cessation"] = period.get("dateDebut")

        return out

    else:
        return {
            "error": f"Failed to retrieve data for SIRET {siret}, status code: {response.status_code}"
        }
