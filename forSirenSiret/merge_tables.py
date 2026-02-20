import pandas as pd

# Helpers to stitch partner base info (names/addresses) onto processed SIREN/SIRET data.

def concat_names(row):
    """
    Concatène les colonnes de nom ('Name', 'Name 2', 'Name 3') en une seule chaîne de caractères.
    Les valeurs NaN ou 'nan' sont ignorées.

    Args:
        row (pd.Series): Une ligne d'un DataFrame.

    Returns:
        str: La chaîne de caractères du nom concaténé.
    """
    names = [row['Name 1'], row['Name 2'], row['Name 3'], row['Name 4']]
    return ' '.join(name for name in names if pd.notna(name) and name != 'nan')

def merge_df(datas : pd.DataFrame, infos_part : pd.DataFrame):
    """
    Fusionne le DataFrame des données traitées (SIREN/SIRET) avec le DataFrame
    contenant des informations supplémentaires sur les partenaires.

    Le processus implique :
    1. Le filtrage du DataFrame `infos_part` pour ne conserver que les partenaires français
       appartenant à des groupes de regroupement spécifiques.
    2. La concaténation des colonnes de nom et de rue.
    3. La réalisation d'une fusion externe (`outer merge`) pour s'assurer que tous les partenaires
       des deux DataFrames sont inclus dans le résultat.

    Args:
        datas (pd.DataFrame): Le DataFrame contenant les données SIREN/SIRET traitées.
        infos_part (pd.DataFrame): Le DataFrame contenant les informations supplémentaires sur les partenaires.

    Returns:
        pd.DataFrame: Le DataFrame fusionné et nettoyé.
    """

    #infos_part = infos_part[infos_part["Country/Region Key"] == "FR"]

    # Renommer la colonne pour correspondre à la clé de fusion et effectuer la fusion.
    infos_part = infos_part.rename(columns={infos_part.columns[0]: "BP"})
    merged = pd.merge(datas, infos_part, on='BP', how='left')
    
    # Filtrer pour ne conserver que les groupes pertinents (ZG0-ZG13, sauf ZG11) et les partenaires français.
    if "Grp." in merged.columns:
        grp_series = merged["Grp."].astype(str)
        grouping_num = pd.to_numeric(grp_series.str[2:], errors="coerce")
        mask = (
            grp_series.str.startswith("ZG")
            & (grouping_num >= 1)
            & (grouping_num <= 13)
            & (grouping_num != 11)
        )
        # If the mask matches nothing (e.g., all NaN after merge), keep rows to avoid empty output.
        if mask.any():
            merged = merged[mask]


    # Appliquer les fonctions de concaténation et supprimer les colonnes d'origine.
    infos_part['Name 1'] = infos_part.apply(concat_names, axis=1)
    merged = merged.drop(columns=['Name 2', 'Name 3', 'Name 4'])
    merged = merged.replace("None", None)
    merged = merged.replace("", None)
    merged = merged.replace("nan", None)
    merged = merged.replace("Nan", None)
    merged = merged.replace("NaN", None)
    merged = merged.replace("NAN", None)
    merged = merged.replace("N/A", None)
    merged = merged.dropna(subset=["siren", "siret", "VAT"], how="all")

    
    merged = merged[~merged["Name 1"].str.startswith("#", na=False)]
    
    if "First Name" in merged.columns:
        merged = merged[~merged["First Name"].str.startswith("#", na=False)]
    else :
        print("Missing column 'First Name' in dataframe")
    if "Last Name" in merged.columns:
        merged = merged[~merged["Last Name"].str.startswith("#", na=False)]
    else :
        print("Missing column 'Last Name' in dataframe")
    if "Search Term 1" in merged.columns:
        merged = merged[~merged["Search Term 1"].str.startswith("#", na=False)]
    else :
        print("Missing column 'Search Term 1' in dataframe")
    merged = merged.astype(str)

    return merged

