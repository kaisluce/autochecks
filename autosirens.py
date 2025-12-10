import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

import partner_processing as pp
from checks import generate_report


load_dotenv()

def create_paths():
    directory = Path(os.getenv("DIRECTORY_LOCATION", "")).expanduser()
    input_file = os.getenv("INPUT_FILE")
    names_file = os.getenv("NAMES_FILE")
    today = datetime.now().strftime("%Y-%m-%d_%H-%M_VAT")
    output_dir = directory / today
    output_dir.mkdir(parents=True, exist_ok=True)
    input_path = os.path.join(directory, input_file)
    names_path = os.path.join(directory, names_file)
    return input_path, names_path, output_dir


def detect_skiprows(file_path: Path) -> int:
    """Ignore une éventuelle ligne d'en-tête 'UNKNOWN TEXT' en début de fichier."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            first = f.readline().strip()
    except OSError:
        return 0
    return 1 if "UNKNOWN" in first.upper() else 0


def main():
    input_path, names_path, output_dir = create_paths()
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
    print(df.head())
    df = df[df["type"].isin(["FR1", "FR2"])].copy()
    df = (
        df.pivot_table(
            index="BP",
            columns="type",
            values="value",
            aggfunc="first",
        )
        .reset_index()
        .rename(columns={"FR1": "siret", "FR2": "siren"})
    )
    print(df.head())

    merged, merged_path = pp.build_partner_dataset(df=df, infos_path=names_path, output_dir=output_dir)
    generate_report(merged, output_dir)


if __name__ == "__main__":
    main()
