import pandas as pd
from pathlib import Path
import logging
from requests.exceptions import RequestException

if __name__ == "__main__":
    from seacrh_name import get_name_from_nif
    from compare_names import compare_names
else:
    from fetchNames.seacrh_name import get_name_from_nif
    from fetchNames.compare_names import compare_names


OUTPUT = Path(r"C:\Users\K.luce\Downloads\checked names.xlsx")

NAME_SLIGHT_THRESHOLD = 70

index = 1
nb_bp = 0


def fetch_names(row, logger=None):
    """
    Docstring for fetch_names
    Function that uses the VAT number of a partner to get its denomination if missing and saves it in a row that will be stored in th DF
    
    :param row: Input row with the differents information columns
    :param logger: Logger used to debug and keep track of the execution
    """
    global index    #index global variable, used to keep track of the progress

    #assigning print or theloger functions depending of if we have the logger or not
    _debug = logger.debug if logger else print
    _warn = logger.warn if logger else print

    nif = row.get("VAT Number")
    country_code = str(row.get("MS Code", "")).strip().upper()

    try:
        name = get_name_from_nif(nif, country_code)   #calls the function that fetch the name of the bp
    except RequestException as exc:
        _warn(f"Erreur reseau lors du fetch pour {country_code}{nif}: {exc}")
        index += 1
        return row
    except RuntimeError as exc:
        _warn(f"Echec HTTP lors du fetch pour {country_code}{nif}: {exc}")
        index += 1
        return row
    except Exception as exc:
        _warn(f"Erreur inattendue lors du fetch pour {country_code}{nif}: {exc}")
        index += 1
        return row

    #writes down th name if it's been fetched
    if name:
        company = name.strip()
        _debug(f"{index}/{nb_bp} - {nif} - Titre trouve: {company}")
        row["Name"] = company
    else:
        _warn(f"Titre introuvable for {nif}")
    index += 1  #increments the index to keep track of the progress
    return row

def main(vatDf : pd.DataFrame, DatasDf : pd.DataFrame, sirenDF : pd.DataFrame, logger=None):
    global nb_bp
    _log = logger.log if logger else print
    _debug = logger.debug if logger else print
    
    _debug(DatasDf.describe(include='all'))
    
    #filters the vat df o extract riws with names to fill
    toSearchDf = vatDf[vatDf['MS Code'].isin(['ES', 'DE'])].copy()
    
    #uses fetch_names to get the missing names
    nb_bp = len(toSearchDf)
    _log(f"number of BP to treat: {nb_bp}")
    toSearchDf = toSearchDf.apply(fetch_names, axis=1, logger=logger)
    
    #writes back the names in the DF
    vatDf.loc[toSearchDf.index, "Name"] = toSearchDf["Name"]
    
    #Prepare the DF to be merged with other tables
    #Recreate the VAT crolumn from the MS code and VAT number and drops the useless columns
    vatDf["VAT"] = vatDf["MS Code"].fillna("").astype(str) + vatDf["VAT Number"].fillna("").astype(str)
    vatDf = vatDf[["VAT", "Name"]]
    vatDf = vatDf.rename(columns={"Name" : "Fetched Name"})
    
    # bring fetched names into the dataset using the VAT key
    DatasDf = DatasDf.merge(vatDf, how="left", on="VAT")
    DatasDf = DatasDf.merge(sirenDF, how="left", on="BP", suffixes=("", "_y"))
    
    # If merge created suffixes, restore the base SAP name column for downstream logic.
    if "Name 1" not in DatasDf.columns:
        if "Name 1_x" in DatasDf.columns:
            DatasDf["Name 1"] = DatasDf["Name 1_x"]
        elif "Name 1_y" in DatasDf.columns:
            DatasDf["Name 1"] = DatasDf["Name 1_y"]
    DatasDf = DatasDf.drop(columns=["Name 1_x", "Name 1_y"], errors="ignore")
    
    # Drop duplicated columns from the SIREN/SIRET merge and clean suffixes.
    drop_cols = [
        "VAT_y", "type", "siret_y", "nic", "siren_y", "status", "siege",
        "date_creation", "date_cessation", "siret_siege", "naf", "naf_label", "cat_juridique",
        "adresse", "n_voie", "voie", "code_postal", "commune",
        "duplicates_siren_y", "duplicates_siret_y", "duplicates_VAT_y",
        "missing siren_y", "missing siret_y", "Missing_Vat",
        "uses a snetor siren_y", "uses a snetor siret_y", "uses a snetor VAT_y",
        "Missmatching siren siret_y", "Missmatching siren VAT_y",
        "Country/Region Key", "Language Key", "datas", "report",
    ]
    DatasDf = DatasDf.drop(columns=drop_cols, errors="ignore")
    DatasDf = DatasDf.rename(columns={"VAT_x": "VAT", "siret_x": "siret", "siren_x": "siren"})
    
    #Check for the names similarity
    DatasDf = DatasDf.apply(compare_names, axis=1)
    DatasDf.drop(columns=["denomination"])
    _debug(DatasDf.describe(include='all'))

    # Keep best score per BP, then restore BP order for readability.
    if "name score" in DatasDf.columns and "BP" in DatasDf.columns:
        DatasDf["_name_score_num"] = pd.to_numeric(DatasDf["name score"], errors="coerce").fillna(-1)
        DatasDf = DatasDf.sort_values(by=["_name_score_num"], ascending=False, kind="mergesort")
        DatasDf = DatasDf.drop_duplicates(subset=["BP"], keep="first")
        DatasDf = DatasDf.sort_values(by=["BP"], kind="mergesort").drop(columns=["_name_score_num"])
    
    #Prepare the output with a clean format
    DatasDf["SAP Name"] = DatasDf["Name 1"]
    created_cols = ['Fetched Name', 'SAP Name', 'name match diag', 'name score']
    base_cols = [col for col in DatasDf.columns if col not in created_cols]
    DatasDf = DatasDf[base_cols + [col for col in created_cols if col in DatasDf.columns]]
    
    #Logs/sebugs the different statistics of the results
    total = len(DatasDf)
    match_counts = DatasDf['name match diag'].value_counts()
    exact_pct = (match_counts.get('exact', 0) / total * 100) if total else 0
    slight_pct = (match_counts.get('slight difference', 0) / total * 100) if total else 0
    _log(f"Exact matches: {exact_pct:.2f}%")
    _log(f"Slight differences: {slight_pct:.2f}%")
    _debug(DatasDf.describe(include='all'))
    return DatasDf

if __name__ == "__main__":
    vatDf = pd.read_excel(r"Z:\MDM\998_CHecks\BP-AUTOCHECKS\2026-02-20_03-02_REPORT\vat\report_concatenated.xlsx").astype(str)
    DatasDf = pd.read_excel(r"z:\MDM\998_CHecks\BP-AUTOCHECKS\2026-02-20_03-02_REPORT\latest_datas.xlsx").astype(str)
    SirenDF = pd.read_excel(r"Z:\MDM\998_CHecks\BP-AUTOCHECKS\2026-02-20_03-02_REPORT\siren_siret\latest_report.xlsx").astype(str)
    result = main(vatDf, DatasDf, SirenDF)
    result.to_excel(OUTPUT, index=False)
