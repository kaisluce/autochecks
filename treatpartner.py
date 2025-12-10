import re
from decimal import Decimal, InvalidOperation

import pandas as pd


def _normalize_identifier(value: str):
    """
    Nettoie un identifiant en le forçant en chaîne de chiffres (pas de notation scientifique).
    """
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() == "nan" or s == "":
        return None
    # Replace comma by dot for decimal/exponential parsing.
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
    Traite les données pour un seul partenaire afin d'extraire, de valider et de vérifier
    les identifiants SIREN et SIRET.

    Cette fonction effectue les opérations suivantes :
    1. Isole les lignes du DataFrame appartenant à un partenaire spécifique.
    2. Extrait les valeurs SIREN et SIRET en se basant sur une colonne de spécification (par exemple, 'FR1', 'FR2').
    3. Recherche les doublons, c'est-à-dire d'autres partenaires utilisant le même SIREN ou SIRET.
    4. Effectue des contrôles de validation :
        - Vérifie si des valeurs sont manquantes.
        - Vérifie la cohérence entre SIRENet SIRET.
        - Valide la longueur des identifiants.
        - Vérifie si les identifiants appartiennent à une liste prédéfinie (Snetor).

    Args:
        partner (str): L'identifiant du partenaire à traiter.
        df (pd.DataFrame): Le DataFrame complet contenant les données de tous les partenaires.

    Returns:
        dict: Un dictionnaire contenant les informations extraites et les résultats de la validation pour le partenaire.
    """
    # sets the returned values to None first, normalizing to avoid scientific notation
    siren_series = df.loc[df["BP"] == partner, "siren"].astype(str)
    siret_series = df.loc[df["BP"] == partner, "siret"].astype(str)

    siren = _normalize_identifier(siren_series.iloc[0] if not siren_series.empty else None)
    siret = _normalize_identifier(siret_series.iloc[0] if not siret_series.empty else None)
    
    doubles_siret = df.loc[(df["siret"] == siret) & (df["BP"] != partner), "BP"].tolist()
    doubles_siren = df.loc[(df["siren"] == siren) & (df["BP"] != partner), "BP"].tolist()
    
    # Initialise le dictionnaire de sortie avec un statut de validité par défaut.
    out =  {"partner" : partner, "siren" : siren, "siret" : siret, "duplicates_siren" : doubles_siren, "duplicates_siret" : doubles_siret, "valid" : 'All good'}
    
    # Si l'un des identifiants est manquant, le statut de validité est mis à jour pour refléter les valeurs manquantes.
    if siren == None or siret == None:
        out['valid'] = 'Missing values :'
        if siren == None:
            out['valid'] += 'siren '
        if siret == None:
            out['valid'] += 'siret '
        out['valid'] += '; '
    else: 
        # Teste si le SIREN (9 premiers chiffres du SIRET) correspondent.
        if siret[:9] != siren:
            out['valid'] = 'Missmatching siren siret'
        
    # tests the lengths of each identifier to see if it is inputed right
    # Valide la longueur de chaque identifiant. Si elle est incorrecte, marque l'entrée comme invalide.
    if siren != None and len(siren) != 9:
        print(siren, len(siren))
        out['siren'] = f"Invalid input ({siren})"
        out["valid"] += "Invalid siren lenght"
        input_ln_issue = True
    if siret != None and len(siret) != 14:
        out['siret'] = f"Invalid input ({siret})"
        out["valid"] += "invalid siret lenght"
    
    # Vérifie si l'un des identifiants est associé à Snetor.
    if checkSnetor(siren):
        out['valid'] += 'uses a snetor siren ; '
    if checkSnetor(siret):
        out['valid'] += 'uses a snetor siret ; ' 
    return out

def checkSnetor(input):
    """
    Vérifie si un identifiant donné (SIREN ou SIRET) appartient à une liste
    prédéfinie d'identifiants liés à Snetor.

    Args:
        input (str): La chaîne de l'identifiant à vérifier.

    Returns:
        bool: True si l'identifiant est trouvé dans la liste Snetor, sinon False.
    """
    if input != None:
        if len(input) == 9:
            for vat in (snetor_VAT):
                if input in vat:
                    return True
        elif len(input) == 14:
            for vat in snetor_VAT:
                if input[:9] in vat:
                    return True
    return False
    
# Liste de tous les numéros de TVA de Snetor à vérifier dans le rapport.
snetor_VAT = ['BE0749997169', 'NL825733273B01', 'BE0899097750', 'DE325236398', 'SI49978608', 'BE0782782575',
              'GB134706719', 'FR25784158545', 'FR72822870226', 'FR75383926409', 'FR36411096290', 'FR58300960622',
              'BE0476868529', 'DE322263583']
