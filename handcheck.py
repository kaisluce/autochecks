import os
import sys
import queue
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

import mail_export as me


load_dotenv()

def create_paths():
    directory = Path(os.getenv("DIRECTORY_LOCATION", "")).expanduser()
    input_location = os.getenv("INPUTS")
    input_file = os.getenv("INPUT_FILE")
    names_file = os.getenv("NAMES_FILE")
    join_tabme = os.getenv("JOIN_TABLE")
    adress_table = os.getenv("ADRESS_TABLE")
    today = datetime.now().strftime("%Y-%m-%d_%H-%M_HANDCHECK_REPORT")
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
    """Skip an optional leading header line that contains 'UNKNOWN TEXT'."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            first = f.readline().strip()
    except OSError:
        return 0
    return 1 if "UNKNOWN" in first.upper() else 0


class TkLogViewer:
    """Small log panel that captures stdout/stderr and displays them in Tk."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.queue: queue.Queue = queue.Queue()
        self.status = tk.StringVar(value="Waiting for a SAP BP CSV file.")
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
    try:
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
        df = pd.read_csv(
            target_input,
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
        )
        if "VAT" not in df.columns:
            df["VAT"] = ""

        logger.update_status("Building partner dataset...")
        merged, _ = pp.build_partner_dataset(
            df=df,
            infos_path=names_path,
            output_dir=output_dir,
            join_table_path=join_path,
            adress_table_path=adress_path,
            update_status=logger.update_status,
        )
        merged = merged.head(20)

        siren_df = merged[
            ~(merged["VAT"].isna() | merged["VAT"].astype(str).str.startswith("FR") | merged["VAT"] == "None")
        ]

        siren_thread = threading.Thread(
            target=generate_report,
            kwargs={
                "output": siren_df,
                "input_dir": output_dir,
                "output_dir": siren_directory,
                "update_status": logger.update_status,
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
            },
            daemon=True,
        )
        logger.update_status("Starting SIREN/SIRET and VAT checks...")
        siren_thread.start()
        vat_thread.start()
        siren_thread.join()
        vat_thread.join()

        logger.update_status("Preparing final reports...")
        me.main(output_dir)
        logger.update_status("Processing completed.")
    except Exception as exc:
        logger.update_status(f"Error: {exc}")


if __name__ == "__main__":
    root = tk.Tk()
    root.title("Autochecks")
    logger = TkLogViewer(root)

    def select_and_run():
        input_location = os.getenv("INPUTS", "")
        selected = filedialog.askopenfilename(
            title="Select an entry file to process (SAP BP export)",
            filetypes=[("CSV Files", "*.csv")],
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
        text="Pick SAP BP CSV and run",
        command=select_and_run,
    ).pack(fill="x", padx=10, pady=(5, 0))

    root.mainloop()
