import os

import openpyxl
import pandas as pd

from logger import log_helpers

FILESIZE = 50

def reformate(
    df: pd.DataFrame,
    column: str,
    output_dir: str,
    requester_vat: str = "FR75383926409",
    progress_callback=None,
    logger=None,
):
    """
    Split the VAT column into batches of FILESIZE rows and write CSV batch files.

    :param df: Source dataframe containing the VAT column.
    :param column: Name of the VAT column to process.
    :param output_dir: Base directory where the ``data`` folder will be created.
    :param filepath: Original source filepath (used for naming output parts).
    :param requester_vat: VAT number of the requester.
    :param progress_callback: Optional callable receiving progress messages.
    """
    def _process_vat_column(values, new_df, num):
        for value in values:
            if value:
                country_code = value[:2]
                searched_vat = value[2:]
                newline = pd.DataFrame(
                    [
                        {
                            "MS Code": country_code,
                            "VAT Number": searched_vat,
                            "Requester MS Code": rq_c_code,
                            "Requester VAT Number": rq_vat_number,
                        }
                    ]
                )
                new_df = pd.concat([new_df, newline])
                if num % FILESIZE == 0:
                    output_file = os.path.join(data_dir, f"{output_base}_part{(num - 1) // FILESIZE:03d}.csv")
                    _log(f"{num} Saving {output_file} with {len(new_df)} entries")
                    new_df.to_csv(output_file, index=False)
                    new_df = pd.DataFrame(columns=["MS Code", "VAT Number", "Requester MS Code", "Requester VAT Number"])
                num += 1
        return new_df, num
    
    _debug, _log, _warn, _error = log_helpers(logger)
    progress = progress_callback or (lambda message: None)
    data_dir = os.path.join(output_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    if column not in df.columns:
        df = df.copy()
        df[column] = ""

    _debug(f"Input rows: {len(df)}")
    df = df.copy()
    df.dropna(subset=[column], inplace=True)
    df = df[df[column] != "None"]
    df = df.drop_duplicates(subset=["VAT"])
    progress(f"Reformatting data into {((len(df)) // FILESIZE) + 1} files.")
    _debug(f"Rows after VAT dropna: {len(df)}")

    new_df = pd.DataFrame(columns=["MS Code", "VAT Number", "Requester MS Code", "Requester VAT Number"])
    rq_c_code = requester_vat[:2]
    rq_vat_number = requester_vat[2:]
    num = 1
    output_base = os.path.splitext(os.path.basename("vat_data"))[0]

    new_df, num = _process_vat_column(df[column], new_df, num)
    if not new_df.empty:
        new_line = pd.DataFrame(
            [
                {
                    "MS Code": "XX",
                    "VAT Number": "XXXXXXXXXXXXXX",
                    "Requester MS Code": rq_c_code,
                    "Requester VAT Number": rq_vat_number,
                }
            ]
        )
        while new_df.shape[0] <= 3:
            new_df = pd.concat([new_df, new_line])
        output_file = os.path.join(data_dir, f"{output_base}_part{(num - 1) // FILESIZE:03d}.csv")
        _log(f"Saving {output_file} with {len(new_df)} entries")
        new_df.to_csv(output_file, index=False)
    progress(f"submitting {(num // FILESIZE) + 1} files.")
