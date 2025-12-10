import pandas as pd
import os
from dotenv import load_dotenv
import msal
import requests

load_dotenv()

DIRECTORY_LOCATION = os.path.join(os.getenv("DIRECTORY_LOCATION", ""), "2025-12-09_15-01_VAT")


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

def save_df(df : pd.DataFrame, path : str):
    with pd.ExcelWriter(path, engine="xlsxwriter", mode = "w") as writer:
        # Crée une feuille vide avec les en-têtes de colonnes corrects.
        df.to_excel(
            writer, index=False, sheet_name="Report", header=True
        )

        workbook = writer.book
        worksheet = writer.sheets["Report"]

        # --- Définitions des formats de couleur ---
        green = workbook.add_format({"bg_color": "#C6EFCE"})   # OK
        orange = workbook.add_format({"bg_color": "#FAB370"})  # missing / mismatch
        red = workbook.add_format({"bg_color": "#F4CCCC"})     # not active / invalid
        yellow = workbook.add_format({"bg_color": "#FAE484"})  # active + all good + duplicates
        dark_orange = workbook.add_format({"bg_color": "#BB9255"})

        # --- Règles de formatage conditionnel ---
        # Ces règles colorent les lignes en fonction de combinaisons de statuts, de validité et de doublons.
        # La plage est définie de manière très large pour couvrir la plupart des cas d'utilisation.
        range_ref = f"A2:AI{200000}"

        worksheet.conditional_format(
            range_ref,
            {
                "type": "formula",
                "criteria": (
                    '=AND('
                    'OR($F2="Actif",$F2="Active"),'
                    '$W2="All good",'
                    'OR($U2<>"[]",$V2<>"[]")'
                    ')'
                ),
                "format": yellow,
                "stop_if_true": True,
            },
        )

        worksheet.conditional_format(
            range_ref,
            {
                "type": "formula",
                "criteria": (
                    '=AND('
                    'OR($F2="Actif",$F2="Active"),'
                    '$W2<>"All good",'
                    '$U2="[]",'
                    '$V2="[]"'
                    ')'
                ),
                "format": orange,
                "stop_if_true": True,
            },
        )

        worksheet.conditional_format(
            range_ref,
            {
                "type": "formula",
                "criteria": (
                    '=AND('
                    'OR($F2="Actif",$F2="Active"),'
                    '$W2<>"All good"'
                    ')'
                ),
                "format": dark_orange,
                "stop_if_true": True,
            },
        )

        worksheet.conditional_format(
            range_ref,
            {
                "type": "formula",
                "criteria": (
                    '=NOT(OR($F2="Actif",$F2="Active"))'
                ),
                "format": red,
            },
        )

        worksheet.conditional_format(
            range_ref,
            {
                "type": "formula",
                "criteria": '=AND(OR($F2="Actif",$F2="Active"), $W2="All good", $U2="[]", $V2="[]")',
                "format": green,
            },
        )


def main():
    return

if __name__ == "__main__":
    df = pd.read_excel(os.path.join(DIRECTORY_LOCATION, "latest_report.xlsx"))
    closed_path = os.path.join(DIRECTORY_LOCATION, "closed_siret.xlsx")
    stopped_path = os.path.join(DIRECTORY_LOCATION, "stopped_siren.xlsx")
    dupe_path = os.path.join(DIRECTORY_LOCATION, "duplicated_siret.xlsx")
    wrong_path = os.path.join(DIRECTORY_LOCATION, "wrong_name.xlsx")
    
    closed = get_closed_siret(df)
    stopped = get_stopped_siren(df)
    dupe = get_duplicated_siret(df)
    wrong = get_wrong_name(df)

    save_df(closed, closed_path)
    save_df(stopped, stopped_path)
    save_df(dupe, dupe_path)
    save_df(wrong, wrong_path)