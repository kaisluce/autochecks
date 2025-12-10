import pandas as pd

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
    # Filtrer pour ne conserver que les groupes pertinents (ZG0-ZG13, sauf ZG11) et les partenaires français.
    grouping_num = pd.to_numeric(infos_part['Grp.'].str[2:], errors='coerce')
    infos_part = infos_part[infos_part['Grp.'].str.startswith('ZG') & (grouping_num >= 0) & (grouping_num <= 13) & (grouping_num != 11)]

    #infos_part = infos_part[infos_part["Country/Region Key"] == "FR"]

    # Renommer la colonne pour correspondre à la clé de fusion et effectuer la fusion.
    infos_part = infos_part.rename(columns={'Business Partner': 'BP'})
    merged = pd.merge(datas, infos_part, on='BP', how='outer')

    merged['datas'] = 'Everything is valid'

    # Appliquer les fonctions de concaténation et supprimer les colonnes d'origine.
    infos_part['Name 1'] = infos_part.apply(concat_names, axis=1)
    merged = merged.drop(columns=['Name 2', 'Name 3', 'Name 4'])
    merged = merged.dropna(subset=['siren', 'siret'], how='all')
    merged = merged[~merged["Name 1"].str.startswith("#", na=False)]
    merged = merged.astype(str)

    return merged