import os
from pathlib import Path
import asyncio

import pandas as pd
from dotenv import load_dotenv

if __name__ == "__main__":
    import mailtemplate as mailtemplate
else:
    import emailing.mailtemplate as mailtemplate

load_dotenv()

DIRECTORY_LOCATION = os.path.join(os.getenv("DIRECTORY_LOCATION", ""), "2025-12-10_12-03_REPORT")

ID_COLUMNS = ("BP", "Business Partner", "siren", "siret")

def _coerce_id_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ID_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def get_closed_siret(df: pd.DataFrame):
    closed = df[df["status"] == "Fermé"]
    return closed

def get_stopped_siren(df: pd.DataFrame):
    stopped = df[df["status"] == "Cessée"]
    return stopped

def get_duplicated_siret(df: pd.DataFrame):
    dupe = df[df["duplicates_siret"]!="[]"]
    return dupe

def get_wrong_name(df: pd.DataFrame):
    wrong = df[df["diagnostic_name"]!="exact"]
    return wrong

def get_bad_VAT(df: pd.DataFrame):
    wrong = df[df["Valid"]=="NO"]
    wrong = wrong[wrong["VAT Number"] != "XXXXXXXXXXXXXX"]
    return wrong

def save_df(df : pd.DataFrame, path : str):
    with pd.ExcelWriter(path, engine="xlsxwriter", mode = "w") as writer:
        # Crée une feuille vide avec les en-têtes de colonnes corrects.
        df.to_excel(
            writer, index=False, sheet_name="Report", header=True
        )

def send_with_file(file_path: str) -> None:
    subject = Path(file_path).stem
    asyncio.run(mailtemplate.main(subject, file_path))

def send(subject : str) -> None:
    asyncio.run(mailtemplate.main(subject))



def main(path : str):
    df1_path = os.path.join(path, "siren_siret/latest_report.xlsx")
    vat_path = os.path.join(path, "vat/report_concatenated.xlsx")
    df1 = pd.read_excel(df1_path, dtype=str)
    vat = pd.read_excel(vat_path, dtype=str)
    df1 = _coerce_id_columns(df1)
    vat = _coerce_id_columns(vat)
    closed_path = os.path.join(path, "siren_siret/closed_siret.xlsx")
    stopped_path = os.path.join(path, "siren_siret/closed_siren.xlsx")
    dupe_path = os.path.join(path, "siren_siret/duplicated_siret.xlsx")
    wrong_path = os.path.join(path, "siren_siret/wrong_name.xlsx")
    bad_vat_path = os.path.join(path, "vat/bad_vats.xlsx")
    closed = get_closed_siret(df1)
    stopped = get_stopped_siren(df1)
    dupe = get_duplicated_siret(df1)
    wrong = get_wrong_name(df1)
    bad_vat = get_bad_VAT(vat)

    if not closed.empty:
        save_df(closed, closed_path)
        send_with_file(closed_path)
    else: send("closed_siret")
    if not stopped.empty:
        save_df(stopped, stopped_path)
        send_with_file(stopped_path)
    else: send("closed_siren")
    if not dupe.empty:
        save_df(dupe, dupe_path)
        send_with_file(dupe_path)
    else: send("duplicated_siret")
    if not wrong.empty:
        save_df(wrong, wrong_path)
        send_with_file(wrong_path)
    else: send("wrong_name")
    if not bad_vat.empty:
        save_df(bad_vat, bad_vat_path)
        send_with_file(bad_vat_path)
    else: send("bad_vats")



if __name__ == "__main__":
#     path = DIRECTORY_LOCATION
#     df1_path = os.path.join(path, "siren_siret/latest_report.xlsx")
#     vat_path = os.path.join(path, "vat/report_concatenated.xlsx")
#     df1 = pd.read_excel(df1_path)
#     vat = pd.read_excel(vat_path)
#     closed_path = os.path.join(path, "siren_siret/closed_siret.xlsx")
#     stopped_path = os.path.join(path, "siren_siret/stopped_siren.xlsx")
#     dupe_path = os.path.join(path, "siren_siret/duplicated_siret.xlsx")
#     wrong_path = os.path.join(path, "siren_siret/wrong_name.xlsx")
#     bad_vat_path = os.path.join(path, "vat/bad_vats.xlsx")
#     closed = get_closed_siret(df1)
#     stopped = get_stopped_siren(df1)
#     dupe = get_duplicated_siret(df1)
#     wrong = get_wrong_name(df1)
#     bad_vat = get_bad_VAT(vat)

#     save_df(closed, closed_path)
#     save_df(stopped, stopped_path)
#     save_df(dupe, dupe_path)
#     save_df(wrong, wrong_path)
#     save_df(bad_vat, bad_vat_path)
    path = r"Z:\MDM\998_CHecks\2025-12-22_11-57_HANDCHECK_REPORT"
    df1_path = os.path.join(path, "siren_siret/latest_report.xlsx")
    vat_path = os.path.join(path, "vat/report_concatenated.xlsx")
    df1 = pd.read_excel(df1_path, dtype=str)
    vat = pd.read_excel(vat_path, dtype=str)
    df1 = _coerce_id_columns(df1)
    vat = _coerce_id_columns(vat)
    closed_path = os.path.join(path, "siren_siret/closed_siret.xlsx")
    stopped_path = os.path.join(path, "siren_siret/stopped_siren.xlsx")
    dupe_path = os.path.join(path, "siren_siret/duplicated_siret.xlsx")
    wrong_path = os.path.join(path, "siren_siret/wrong_name.xlsx")
    bad_vat_path = os.path.join(path, "vat/bad_vats.xlsx")
    closed = get_closed_siret(df1)
    stopped = get_stopped_siren(df1)
    dupe = get_duplicated_siret(df1)
    wrong = get_wrong_name(df1)
    bad_vat = get_bad_VAT(vat)

    if not closed.empty:
        send_with_file(closed_path)
    else: send("closed_siret")
    if not stopped.empty:
        send_with_file(stopped_path)
    else: send("stopped_siren")
    if not dupe.empty:
        send_with_file(dupe_path)
    else: send("duplicated_siret")
    if not wrong.empty:
        send_with_file(wrong_path)
    else: send("wrong_name")
    if not bad_vat.empty:
        send_with_file(bad_vat_path)
    else: send("bad_vats")