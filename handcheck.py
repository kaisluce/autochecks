import os
import sys
import queue
import traceback
from datetime import datetime
from pathlib import Path
import threading
import tkinter as tk
from tkinter import filedialog, scrolledtext

import pandas as pd
from dotenv import load_dotenv

import forSirenSiret.partner_processing as pp
from forSirenSiret.checks import generate_report
from forVats.process import process

import emailing.mail_export as me
import logger as filelog

# Base directory and default fallbacks; normally .env overrides these.
BASE_DIR = Path(__file__).resolve().parent
DIRECTORY_LOCATION=r"Z:\MDM\998_CHecks\AUTOCHECKS"
INPUTS = "\\\\interfacessap.file.core.windows.net\\interfacess4p\\data_mdm_export"
INPUT_FILE="BP_TAXNUM.csv"
NAMES_FILE="BP_BUT000.csv"
JOIN_TABLE = "BP_BUT020.csv"
ADRESS_TABLE = "BP_ADRC.csv"

def load_env(logger=None) -> bool:
    """
    Load .env from common locations so the PyInstaller exe also finds it.
    Returns True if a file was loaded.
    """
    exe_dir = Path(sys.argv[0]).resolve().parent
    runtime_dir = Path(getattr(sys, "_MEIPASS", BASE_DIR))
    candidates = [
        BASE_DIR / ".env",
        BASE_DIR.parent / ".env",
        exe_dir / ".env",
        runtime_dir / ".env",
        Path.cwd() / ".env",
    ]
    for path in candidates:
        if path.exists():
            load_dotenv(path, override=True)
            if logger:
                logger.update_status(f".env chargé depuis {path}")
            return True
    load_dotenv(override=False)  # fallback to default behaviour
    if logger:
        logger.update_status("Aucun .env trouvé; variables d'environnement système utilisées.")
    return False


# Load env at import time for scripts; UI run will call again with logger for feedback.
ENV_LOADED = load_env()


def _normalize_bp_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Rename the first three columns to BP/type/value and enforce their presence."""
    if df_raw.shape[1] < 3:
        raise ValueError("Le fichier doit contenir au moins trois colonnes (BP, type, value).")
    col0, col1, col2 = df_raw.columns[:3]
    normalized = df_raw.rename(columns={col0: "BP", col1: "type", col2: "value"})
    return normalized[["BP", "value", "type"]].astype(str)


def create_paths():
    # Build input/output paths using the configured base directories and current timestamp.
    directory = Path(DIRECTORY_LOCATION).expanduser()
    BUT_dir = INPUTS
    input_location = INPUTS
    input_file = INPUT_FILE
    names_file = NAMES_FILE
    join_table = JOIN_TABLE
    adress_table = ADRESS_TABLE
    today = datetime.now().strftime("%Y-%m-%d_%H-%M_HANDCHECK_REPORT")
    output_dir = directory / today
    siren_directory = output_dir / "siren_siret"
    VAT_directory = output_dir / "vat"
    output_dir.mkdir(parents=True, exist_ok=True)
    siren_directory.mkdir(parents=True, exist_ok=True)
    VAT_directory.mkdir(parents=True, exist_ok=True)
    input_path = os.path.join(input_location, input_file)
    names_path = os.path.join(BUT_dir, names_file)
    join_path = os.path.join(BUT_dir, join_table)
    adress_path = os.path.join(BUT_dir, adress_table)
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
    """Skip an optional leading header line that contains 'UNKNOWN TEXT'."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            first = f.readline().strip()
    except OSError:
        return 0
    return 1 if "UNKNOWN" in first.upper() else 0


def load_bp_csv(file_path: str, skip: int, logger: "TkLogViewer") -> pd.DataFrame:
    """
    Read the BP export while handling multiple delimiter/header shapes.

    The classic case is a semicolon-separated file without headers. Some SAP
    exports, however, come with a header row and use commas (or mixed quoting),
    which pandas treats as a single column if we force ``sep=';'``. This helper
    tries the legacy format first, then falls back to an auto-detected delimiter
    with a header row using the French column titles. Excel files (`.xlsx`/`.xls`)
    are also accepted by reusing the first three columns.
    """
    base_kwargs = {
        "dtype": str,
        "skiprows": skip,
        "on_bad_lines": "skip",
        "engine": "python",
    }

    # Excel fallback: accept `.xlsx`/`.xls` inputs with a header row.
    if Path(file_path).suffix.lower() in {".xlsx", ".xls"}:
        try:
            df_raw = pd.read_excel(file_path, dtype=str, engine="openpyxl")
            return _normalize_bp_dataframe(df_raw)
        except Exception as exc:
            logger.update_status(f"Lecture Excel impossible ({exc}); tentative en mode CSV...")

    # Legacy format: semicolon-separated, no header, fixed positions.
    try:
        df = pd.read_csv(
            file_path,
            sep=";",
            header=None,
            usecols=[0, 1, 3],
            names=["BP", "value", "type"],
            **base_kwargs,
        )
        if {"BP", "value", "type"}.issubset(df.columns):
            return df
    except Exception as exc:
        logger.update_status(f"Fallback to auto-detected CSV (legacy read failed: {exc})")

    # Fallback: auto-detect delimiter with header row (e.g., 'Partenaire', 'Cat. N° ID fiscale', ...).
    try:
        df_raw = pd.read_csv(file_path, sep=None, header=0, **base_kwargs)
        # Prioritize position over header names: first 3 columns become BP/type/value
        return _normalize_bp_dataframe(df_raw)
    except Exception as exc:
        logger.update_status(f"Auto-detection failed ({exc}); trying explicit separators...")

    # Last-chance attempts: explicit separators with header row, renaming by index.
    for sep in (";", ","):
        try:
            df_raw = pd.read_csv(file_path, sep=sep, header=0, **base_kwargs)
            return _normalize_bp_dataframe(df_raw)
        except Exception:
            continue

    raise RuntimeError("Impossible de lire le fichier d'entree: aucun format de delimiter n'a fonctionné.")


class TkLogViewer:
    """Small log panel that captures stdout/stderr and displays them in Tk (UI + file/log console)."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.queue: queue.Queue = queue.Queue()
        self.status = tk.StringVar(value="Waiting for a SAP BP CSV or Excel file.")
        self._stdout = sys.stdout
        self._stderr = sys.stderr

        tk.Label(root, text="Execution log").pack(anchor="w", padx=10, pady=(10, 0))
        self.text = scrolledtext.ScrolledText(root, wrap="word", height=20, state="disabled")
        self.text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        tk.Label(root, textvariable=self.status, anchor="w", relief="sunken").pack(fill="x", padx=10, pady=(0, 10))

        sys.stdout = self
        sys.stderr = self
        self.root.after(100, self._drain_queue)

    def write(self, message: str):
        if not message:
            return
        self.queue.put(message)
        if self._stdout:
            self._stdout.write(message)

    def flush(self):
        if self._stdout:
            self._stdout.flush()

    def update_status(self, message: str):
        """Push a line into the log area and refresh the status bar."""
        formatted = f"{message}\n" if not message.endswith("\n") else message
        self.queue.put(formatted)
        self.status.set(message)
        if self._stdout:
            self._stdout.write(formatted)

    def _drain_queue(self):
        while not self.queue.empty():
            msg = self.queue.get_nowait()
            self.text.configure(state="normal")
            self.text.insert(tk.END, msg)
            self.text.see(tk.END)
            self.text.configure(state="disabled")
        self.root.after(100, self._drain_queue)


def run_pipeline(input_path: str, logger: TkLogViewer):
    """Main ETL pipeline: load BP export, build dataset, run SIREN/SIRET + VAT checks, and write reports."""
    run_logger = None
    try:
        # File logger for structured logs (console + file), UI logger for status/messages.
        run_logger = filelog.logger(mail=False)
        (
            _,
            names_path,
            output_dir,
            siren_directory,
            VAT_directory,
            join_path,
            adress_path
        ) = create_paths()

        target_input = input_path or _
        if not target_input:
            logger.update_status("No file selected.")
            return

        logger.update_status(f"Selected file: {target_input}")
        skip = detect_skiprows(target_input)
        df = load_bp_csv(target_input, skip, logger)
        df["value"] = df["value"].astype(str)
        # Re-map VAT/type codes: keep FRx as-is; normalize EU VATs with code ending with 0 to FR0.
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
        df = pd.concat([dffr, dfeu]).sort_values(by=["BP", "type"])
        df = (
            df.pivot_table(
                index="BP",
                columns="type",
                values="value",
                aggfunc="first",
            )
            .reset_index()
            .rename(columns={"FR0": "VAT", "FR1": "siret", "FR2": "siren"})
            .astype(str)
        )
        # After pivot: each BP has a single VAT/siret/siren row.
        if "VAT" not in df.columns:
            df["VAT"] = ""
        if "siret" not in df.columns:
            df["siret"] = ""
        if "siren" not in df.columns:
            df["siren"] = ""

        logger.update_status("Building partner dataset...")
        
        #Fetching all the datas about the valus by merging with other tables
        merged, _ = pp.build_partner_dataset(
            df=df,
            infos_path=names_path,
            output_dir=output_dir,
            join_table_path=join_path,
            address_table_path=adress_path,
            update_status=logger.update_status,
            logger=run_logger,
        )

        vat_clean = merged["VAT"].astype(str).str.strip().str.upper()
        country_series = merged["country"] if "country" in merged.columns else pd.Series("", index=merged.index)
        country_clean = country_series.astype(str).str.strip().str.upper()
        siren_df = merged[
            vat_clean.str.startswith("FR") | country_clean.isin(["FR", "FRANCE"])
        ]
        logger.update_status(f"SIREN/SIRET candidates: {len(siren_df)} rows")

        # Launch parallel checks: SIREN/SIRET API validation and VAT validation.
        siren_thread = threading.Thread(
            target=generate_report,
            kwargs={
                "output": siren_df,
                "input_dir": output_dir,
                "output_dir": siren_directory,
                "update_status": logger.update_status,
                "logger": run_logger,
            },
            daemon=True,
        )
        vat_thread = threading.Thread(
            target=process,
            kwargs={
                "df": merged,
                "vat_column": "VAT",
                "output_dir": VAT_directory,
                "progress_callback": logger.update_status,
                "logger": run_logger,
            },
            daemon=True,
        )
        logger.update_status("Starting SIREN/SIRET and VAT checks...")
        siren_thread.start()
        vat_thread.start()
        siren_thread.join()
        vat_thread.join()

        # Assemble the final Excel outputs and (optionally) send by email.
        logger.update_status("Preparing final reports...")
        me.main(path=output_dir, mail=False, logger=run_logger)
        logger.update_status("Processing completed.")
    except Exception as exc:
        tb = traceback.format_exc()
        if run_logger is not None:
            run_logger.error("Handcheck pipeline failed", exc_info=True)
        logger.update_status(f"Error: {exc}. Voir le traceback dans le log ci-dessous.")
        logger.write(tb)


if __name__ == "__main__":
    root = tk.Tk()
    root.title("Autochecks")
    logger = TkLogViewer(root)

    def select_and_run():
        input_location = os.getenv("INPUTS", "")
        selected = filedialog.askopenfilename(
            title="Select an entry file to process (SAP BP export, CSV or Excel)",
            filetypes=[
                ("CSV or Excel Files", "*.csv *.xlsx *.xls"),
                ("CSV Files", "*.csv"),
                ("Excel Files", "*.xlsx *.xls"),
            ],
            defaultextension=".csv",
            initialdir=input_location or None,
            parent=root,
        )
        if not selected:
            logger.update_status("File selection cancelled.")
            return
        threading.Thread(target=run_pipeline, args=(selected, logger), daemon=True).start()

    tk.Button(
        root,
        text="Pick SAP BP file and run",
        command=select_and_run,
    ).pack(fill="x", padx=10, pady=(5, 0))

    root.mainloop()
