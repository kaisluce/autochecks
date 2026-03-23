import os
import sys
import csv
import pandas as pd
import forSirenSiret.treatpartner as tp
import forSirenSiret.merge_tables as mt

# Partner data prep utilities used before SIREN/SIRET checks.
ID_COLUMNS = ("BP", "partner", "Business Partner", "siren", "siret")
BP_COLUMNS = {"BP", "partner", "Business Partner"}


def _normalize_bp_value(value: str) -> str:
    """
    Normalize BP identifiers by trimming spaces/quotes and left-padding to 10 digits
    when the value is numeric.
    """
    if pd.isna(value):
        return ""
    s = str(value).strip().strip('"').strip("'")
    # Remove a trailing .0 introduced by Excel-like casts
    if s.endswith(".0"):
        s = s[:-2]
    # Only pad when the cleaned value is purely numeric (ignoring spaces/commas), to avoid dropping characters.
    numeric_candidate = (
        s.replace(" ", "")
        .replace("\u00a0", "")  # non-breaking space
        .replace(",", "")
    )
    if numeric_candidate.isdigit():
        return numeric_candidate.zfill(10)
    return s

def _coerce_id_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Force identifier columns to string to avoid scientific notation and truncation."""
    for col in ID_COLUMNS:
        if col in frame.columns:
            if col in BP_COLUMNS:
                frame[col] = frame[col].apply(_normalize_bp_value)
            else:
                # Normalize identifiers to string and trim surrounding spaces/quotes to avoid join mismatches.
                frame[col] = frame[col].astype(str).str.strip().str.strip('"').str.strip()
    return frame


def build_partner_dataset(
    df: pd.DataFrame,
    infos_path: str,
    join_table_path: str,
    address_table_path: str,
    output_dir: str,
    update_status=None,
    logger=None,
):
    # High-level flow: normalize IDs, merge VAT/SIREN/SIRET with partner info, enrich with addresses, emit latest_datas.xlsx.
    """
    Construit un jeu de données consolidé pour les partenaires en traitant un DataFrame d'entrée,
    puis en le fusionnant avec des informations supplémentaires.
    """
    if update_status is None:
        update_status = lambda msg: None

    def _log_info(msg: str):
        if logger is None:
            return
        if hasattr(logger, "log"):
            logger.log(msg)
        elif hasattr(logger, "info"):
            logger.info(msg)
        else:
            try:
                logger(msg)
            except Exception:
                pass

    def _log_warn(msg: str):
        if logger is None:
            return
        if hasattr(logger, "warn"):
            logger.warn(msg)
        elif hasattr(logger, "warning"):
            logger.warning(msg)
        else:
            _log_info(f"[WARN] {msg}")

    def _log_debug(msg: str):
        if logger is None:
            return
        if hasattr(logger, "debug"):
            logger.debug(msg)
        else:
            _log_info(f"[DEBUG] {msg}")
            
    
    def _is_valid(value) -> bool:
        if pd.isna(value):
            return False
        s = str(value).strip()
        return s and s.lower() not in {"nan", "none", "null", ".", "x", "na", "n/a", "naan", "xx", "xxx"} and len(s) >= 3

    def _is_valid_postcode(value) -> bool:
        if not _is_valid(value):
            return False
        s = str(value).strip()
        return s not in {"0000", "00000", "9999", "99999"}
    
    

    def import_raw_csv(path: str) -> pd.DataFrame:
        return pd.read_csv(
            path,
            sep=";",
            dtype=str,
            quoting=csv.QUOTE_NONE,
            on_bad_lines="skip",
            engine="python",
            header=0,
        ).astype(str)

    def load_info_csv(path: str) -> pd.DataFrame:
        df = import_raw_csv(path)
        
        expected_cols = [
            "Business Partner",
            "Grp.",
            "Arch. Flag",
            "Central",
            "AGrp",
            "Search Term 1",
            "Search Term 2",
            "Name 1",
            "Ext. No.",
            "CatP",
            "Name 2",
            "Last Name",
            "First Name",
            "Date",
            "User",
            "Name 3",
            "Name 4",
            "Date.1",
            "User.1",
        ]
        if len(df.columns) >= len(expected_cols):
            df.columns = expected_cols + list(df.columns[len(expected_cols):])
        coerced = _coerce_id_columns(df)
        if "Business Partner" in coerced.columns:
            coerced["Business Partner"] = coerced["Business Partner"].apply(_normalize_bp_value)
        return coerced

    def load_but020_csv(path: str) -> pd.DataFrame:
        but020 = import_raw_csv(path)
        but020 = but020.iloc[:, :2]
        but020.columns = ["Business Partner", "Addr. No."]
        coerced_but020 = _coerce_id_columns(but020)
        coerced_but020 = coerced_but020.sort_values(by=["Business Partner", "Addr. No."], ascending=[True, False])
        droped_but020 = coerced_but020.drop_duplicates(subset=["Business Partner"])
        return droped_but020

    def load_adress_table(path: str) -> pd.DataFrame:
        adrc = import_raw_csv(path)
        coerced_adrc = _coerce_id_columns(adrc)
        if coerced_adrc.empty:
            _log_warn(f"Address table empty or unreadable: {path}")
        elif len(coerced_adrc.columns) < 30:
            _log_warn(f"Address table has unexpected format ({len(coerced_adrc.columns)} columns), skipping address merge.")
            return pd.DataFrame(columns=["Addr. No."])
        else:
            coerced_adrc = coerced_adrc.iloc[:, [0, 4, 5, 11, 20, 26, 29]]
            new_adrc_columns_names = ["Addr. No.", "postcode", "city", "country", "street5", "street", "street4"]
            coerced_adrc.columns = new_adrc_columns_names
            missing_cols = set(["street", "street4", "street5", "city", "postcode", "country"]) - set(coerced_adrc.columns)
            if missing_cols:
                _log_warn(f"Missing address columns: {sorted(missing_cols)}")
        return coerced_adrc.drop_duplicates(subset=["Addr. No."])
    
    def _enrich_row(row: pd.Series, df: pd.DataFrame) -> pd.DataFrame:
        """Fusionne une ligne avec les données SIREN/SIRET calculées par tp.main."""
        partner = row.get("BP")
        part_data = tp.main(partner, df)
        merged_row = row.to_dict()
        merged_row.update(part_data)
        return pd.DataFrame([merged_row]).astype(str)

    def _append_row_to_excel(writer: pd.ExcelWriter, newline: pd.DataFrame) -> None:
        """Écrit une ligne à la suite dans la feuille Sheet1 du fichier Excel ouvert."""
        newline.to_excel(
            writer,
            index=False,
            header=False,
            startrow=writer.sheets["Sheet1"].max_row
        )

    def _build_output(df: pd.DataFrame, output: pd.DataFrame, output_path: str) -> pd.DataFrame:
        """Itère sur df, enrichit chaque partenaire unique et l'écrit incrémentalement dans Excel."""
        done = []
        df_length = len(df)
        update_status("Building partner dataset...")
        _log_info(f"Building partner dataset: {df_length} input rows, output -> {output_path}")
        with pd.ExcelWriter(output_path, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
            for row_id, row in df.iterrows():
                partner = row.get("BP")
                update_status(f"Partenaire {row_id+1}/{df_length} : {partner}")
                if partner not in done:
                    newline = _enrich_row(row, df)
                    output = pd.concat([output, newline], ignore_index=True)
                    _append_row_to_excel(writer, newline)
                done.append(partner)
        return output
    

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "latest_datas.xlsx")

    output = pd.DataFrame().astype(str)
    output.to_excel(output_path, index=False)
    _log_debug(f"Initialized datas file at {output_path}")

    # Lecture du fichier infos (CSV séparé par ;), en ignorant les lignes malformées.
    infos = load_info_csv(infos_path)
    
    print(infos.describe())
    df = _coerce_id_columns(df)
    if "BP" in df.columns:
        df["BP"] = df["BP"].apply(_normalize_bp_value)
    df = mt.merge_df(df, infos)
    _log_debug(f"Merged base dataframe with infos : \n{df.describe()}")
    
    join_table = load_but020_csv(join_table_path)
    
    address_table = load_adress_table(address_table_path)
    
    address_table = pd.merge(left=join_table, right=address_table, on="Addr. No.", how="left")
    
    # Normaliser les types pour la jointure adresse.
    if not address_table.empty:
        address_table.iloc[:, 0] = address_table.iloc[:, 0].astype(str)

    output = _build_output(df, output, output_path)
    
    # Filtre optionnel : exclure les lignes dont le nom contient "snetor".
    name_col = "Name 1" if "Name 1" in output.columns else "Name1"
    if name_col in output.columns:
        mask1 = ~output[name_col].str.lower().fillna("").str.contains("snetor")
        mask2 = ~output[name_col].str.lower().fillna("").str.contains("gazechim")
        mask3 = ~output[name_col].str.upper().fillna("").str.contains("OZYANCE")
        output = output.loc[mask1]
        output = output.loc[mask2]
        output = output.loc[mask3]

    output = output.drop(
        columns=[
            "Arch. Flag",
            "Central", "AGrp",
            "Search Term 1",
            "Search Term 2",
            "External BP Number",
            "BPC",
            "Ext. No.",
            "CatP",
            "Last Name",
            "First Name",
            "Date",
            "User",
            "Date.1",
            "User.1",
            "Unnamed: 19"
        ],
        errors="ignore")
    check_cols = [col for col in output.columns if col != "BP"]
    _log_debug(output)
    address_table.rename(columns={address_table.columns[0]: "BP"}, inplace=True)
    output.rename(columns={output.columns[0]: "BP"}, inplace=True)
    # Enrichir avec les adresses sans créer de nouvelles lignes (BP manquants à gauche sont ignorés).
    output = pd.merge(left=output, right=address_table, on="BP", how="left")
    output = output.dropna(subset=check_cols, how="all")
    
    output["has_street"] = output["street"].apply(_is_valid)
    output["has_street4"] = output["street4"].apply(_is_valid)
    output["has_street5"] = output["street5"].apply(_is_valid)
    output["has_city"] = output["city"].apply(_is_valid)
    output["has_postcode"] = output["postcode"].apply(_is_valid_postcode)
    output["has_country"] = output["country"].apply(_is_valid)
    
    output.to_excel(output_path, index=False)
    _log_info(f"Final partner dataset saved: {output_path} ({len(output)} rows)")

    return output, output_path

        