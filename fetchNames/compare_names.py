import pandas as pd
from thefuzz import fuzz


NAME_SLIGHT_THRESHOLD = 70

def compare_names(row):
    """
    Compare le nom de l'entreprise provenant de l'API ('denomination') avec le nom
    provenant du fichier d'informations du partenaire ('Name 1') en utilisant la correspondance floue.

    Args:
        row (pd.Series): Une ligne du DataFrame contenant les colonnes 'denomination' et 'Name 1'.

    Returns:
        str: 'exact' pour une correspondance parfaite, 'slight difference' pour une correspondance proche,
             'no match' pour une faible correspondance, ou 'Missing name' si l'un des noms est manquant.
    """
    name1 = row.get('Fetched Name')
    name2 = row.get('Name 1')
    if pd.isna(name2) or name2 == 'nan':
        row['name match diag'] = 'Missing name'
        row['name score'] = 0
        return row
    if pd.isna(name1) or name1 == 'nan' or name1 == '' or name1 == '---' or name1 ==  None:
        row['name match diag'] =  'Name not fetched'
        row['name score'] = 0
        return row
    
    score = fuzz.token_set_ratio(name1, name2)
    if score == 100:
        row['name match diag'] =  'exact'
        row['name score'] = score
    elif score > NAME_SLIGHT_THRESHOLD:
        row['name match diag'] =  'slight difference'
        row['name score'] = score
    else:
        row['name match diag'] =  'no match'
        row['name score'] = score
    return row