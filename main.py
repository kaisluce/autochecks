import os
from datetime import datetime
from pathlib import Path
import threading

import pandas as pd
from dotenv import load_dotenv

import forSirenSiret.partner_processing as pp
from forSirenSiret.checks import generate_report
from forVats.process import process

import mail_export as me


load_dotenv()

def create_paths():
    directory = Path(os.getenv("DIRECTORY_LOCATION", "")).expanduser()
    input_file = os.getenv("INPUT_FILE")
    names_file = os.getenv("NAMES_FILE")
    today = datetime.now().strftime("%Y-%m-%d_%H-%M_REPORT")
    output_dir = directory / today
    siren_directory = output_dir / "siren_siret"
    VAT_directory = output_dir / "vat"
    output_dir.mkdir(parents=True, exist_ok=True)
    siren_directory.mkdir(parents=True, exist_ok=True)
    VAT_directory.mkdir(parents=True, exist_ok=True)
    input_path = os.path.join(directory, input_file)
    names_path = os.path.join(directory, names_file)
    return input_path, names_path, output_dir, siren_directory, VAT_directory


def detect_skiprows(file_path: Path) -> int:
    """Ignore une éventuelle ligne d'en-tête 'UNKNOWN TEXT' en début de fichier."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            first = f.readline().strip()
    except OSError:
        return 0
    return 1 if "UNKNOWN" in first.upper() else 0


def main():
    input_path, names_path, output_dir, siren_directory, VAT_directory= create_paths()
    skip = detect_skiprows(r"\\interfacessap.file.core.windows.net\interfacess4p\data_mdm_export\BP_TAXNUM.csv")
    df = pd.read_csv(
        r"\\interfacessap.file.core.windows.net\interfacess4p\data_mdm_export\BP_TAXNUM.csv",
        sep=";",
        header=None,
        skiprows=skip,
        dtype=str,
        usecols=[0, 1, 3],
        names=["BP", "value", "type"],
        on_bad_lines="skip",
        engine="python",
    )
    df["value"] = df["value"].astype(str)
    dffr = df[df["type"].isin(["FR0", "FR1", "FR2"])].copy()
    dfeu = df[df["type"].isin([
        "DE0",
        "AT0",
        "BE0",
        "BG0",
        "CY0",
        "HR0",
        "DK0",
        "ES0",
        "EE0",
        "FI0",
        "GR0",
        "IE0",
        "IT0",
        "LV0",
        "LT0",
        "LU0",
        "MT0",
        "NL0",
        "PL0",
        "PT0",
        "CZ0",
        "RO0",
        "SK0",
        "SI0",
        "SE0",
        ])]
    mask = dfeu["type"].str.endswith("0")
    dfeu.loc[mask, "type"] = "FR0"
    df = pd.concat([dffr, dfeu]).sort_values(by=["BP", "type"])
    df = (
        df.pivot_table(
            index="BP",
            columns="type",
            values="value",
            aggfunc="first",
        )
        .reset_index()
        .rename(columns={"FR0" : "VAT", "FR1": "siret", "FR2": "siren"})
    )
    # Ensure the VAT column exists even if FR0 was missing from the source file
    if "VAT" not in df.columns:
        df["VAT"] = ""

    merged, merged_path = pp.build_partner_dataset(df=df, infos_path=names_path, output_dir=output_dir)
    print(merged["VAT"].describe())
    merged = merged.head(250)
    # Run SIREN/SIRET and VAT flows in parallel threads
    siren_df = merged[
        ~(merged["VAT"].isna() | merged["VAT"].astype(str).str.startswith("FR") | merged["VAT"] == "None")
    ]

    siren_thread = threading.Thread(
        target=generate_report,
        kwargs={"output": siren_df, "input_dir": output_dir, "output_dir": siren_directory},
    )
    vat_thread = threading.Thread(
        target=process,
        kwargs={"df": merged, "vat_column": "VAT", "output_dir": VAT_directory},
    )
    siren_thread.start()
    vat_thread.start()
    siren_thread.join()
    vat_thread.join()
    
    me.main(output_dir)
    


if __name__ == "__main__":
    main()
