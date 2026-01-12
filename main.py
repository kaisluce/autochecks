import os
from datetime import datetime
from pathlib import Path
import threading

import pandas as pd
from dotenv import load_dotenv

import forSirenSiret.partner_processing as pp
from forSirenSiret.checks import main as SSmain
import forVats.process as vat_process
import logger as log

import emailing.mail_export as me


load_dotenv()

def create_paths():
    """Resolve all inputs/outputs (env-driven) and create date-stamped directories."""
    directory = Path(os.getenv("DIRECTORY_LOCATION", "")).expanduser()
    input_location = Path(os.getenv("INPUTS")).expanduser()
    input_file = os.getenv("INPUT_FILE")
    names_file = os.getenv("NAMES_FILE")
    join_tabme = os.getenv("JOIN_TABLE")
    adress_table = os.getenv("ADRESS_TABLE")
    today = datetime.now().strftime("%Y-%m-%d_%H-%M_REPORT")
    output_dir = directory / today
    siren_directory = output_dir / "siren_siret"
    VAT_directory = output_dir / "vat"
    output_dir.mkdir(parents=True, exist_ok=True)
    siren_directory.mkdir(parents=True, exist_ok=True)
    VAT_directory.mkdir(parents=True, exist_ok=True)
    input_path = os.path.join(input_location, input_file)
    names_path = os.path.join(input_location, names_file)
    join_path = os.path.join(input_location, join_tabme)
    adress_path = os.path.join(input_location, adress_table)
    return (
        input_path,
        names_path,
        output_dir,
        siren_directory,
        VAT_directory,
        join_path,
        adress_path
        )


def detect_skiprows(file_path: Path) -> int:
    """Ignore une éventuelle ligne d'en-tête 'UNKNOWN TEXT' en début de fichier."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            first = f.readline().strip()
    except OSError:
        return 0
    return 1 if "UNKNOWN" in first.upper() else 0


def main():
    
    logger = log.logger(mail=True)
    logger.log("Logger succesfully created")
    logger.log("Starting Process")
    try:

        (
            input_path,
            names_path,
            output_dir,
            siren_directory,
            VAT_directory,
            join_path,
            adress_path
            )= create_paths()
        logger.debug(f"Paths resolved: input={input_path} names={names_path} join={join_path} adress={adress_path} out={output_dir}")
        skip = detect_skiprows(
            input_path
            )
        if skip:
            logger.warn(f"Skiprows detected ({skip}) for input file, possible header anomaly.")
        df = pd.read_csv(
            input_path,
            sep=";",
            header=None,
            skiprows=skip,
            dtype=str,
            usecols=[0, 1, 3],
            names=["BP", "value", "type"],
            on_bad_lines="skip",
            engine="python",
        )
        logger.debug(f"Raw input loaded: {len(df)} rows")
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
            "HU0"
            ])]
        mask = dfeu["type"].str.endswith("0")
        dfeu.loc[mask, "type"] = "FR0"
        # Merge domestic (FR) and EU VAT rows, then reshape to a single row per BP.
        df = pd.concat([dffr, dfeu]).sort_values(by=["BP", "type"])
        df = (
            df.pivot_table(
                index="BP",
                columns="type",
                values="value",
                aggfunc="first",
            )
            .reset_index()
            .rename(columns={"FR0" : "VAT", "FR1": "siret", "FR2": "siren"}).astype(str)
        )
        # Ensure the VAT column exists even if FR0 was missing from the source file
        if "VAT" not in df.columns:
            df["VAT"] = ""
            logger.warn("VAT column missing after pivot; created empty VAT column.")

        merged, merged_path = pp.build_partner_dataset(
            df=df,
            infos_path=names_path,
            output_dir=output_dir,
            join_table_path=join_path,
            address_table_path=adress_path,
            logger=logger,
            )
        logger.log(f"Partner dataset built: {merged_path} ({len(merged)} rows)")
        # print(merged.describe())
        # Prepare subset needing SIREN/SIRET verification (skip pure FR VATs), then run both flows in parallel.
        # Normalize VAT/country before filtering to avoid misses due to casing/spacing.
        vat_clean = merged["VAT"].astype(str).str.strip().str.upper()
        country_clean = merged["country"].astype(str).str.strip().str.upper()
        siren_df = merged[
            vat_clean.str.startswith("FR") | country_clean.isin(["FR", "FRANCE"])
        ]
        siren_df = siren_df.head(90)
        merged = merged.head(90)
        logger.debug(siren_df.describe())
        logger.debug(f"SIREN/SIRET candidate rows: {len(siren_df)} / {len(merged)}")
        siren_thread = threading.Thread(
            target=SSmain,
            kwargs={"output": siren_df, "input_dir": output_dir, "output_dir": siren_directory, "logger": logger},
        )
        vat_thread = threading.Thread(
            target=vat_process.process,
            kwargs={"df": merged, "vat_column": "VAT", "output_dir": VAT_directory, "logger": logger},
        )
    except Exception as exc:
        logger.error("Unexpected error", exc_info=True)
        raise
    # Kick off both flows in parallel; main thread waits before emailing reports.
    siren_thread.start()
    vat_thread.start()
    siren_thread.join()
    vat_thread.join()
    try:
        logger.log("SIREN/SIRET and VAT processing completed")
        # Export anomalies and optionally send by email.
        me.main(output_dir, logger=logger)
    except Exception as exc:
        logger.error("Unexpected error while making final reports", exc_info=True)
        raise
    


if __name__ == "__main__":
    main()
