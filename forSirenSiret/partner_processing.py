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

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "latest_datas.xlsx")

    output = pd.DataFrame().astype(str)
    output.to_excel(output_path, index=False)
    _log_debug(f"Initialized datas file at {output_path}")

    # Lecture du fichier infos (CSV séparé par ;), en ignorant les lignes malformées.
    infos = pd.read_csv(
        infos_path,
        sep=";",
        dtype=str,
        quoting=csv.QUOTE_NONE,
        on_bad_lines="skip",
        engine="python",
        header=0,
    ).astype(str)
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
    if len(infos.columns) >= len(expected_cols):
        infos.columns = expected_cols + list(infos.columns[len(expected_cols):])
    infos = _coerce_id_columns(infos)
    if "Business Partner" in infos.columns:
        infos["Business Partner"] = infos["Business Partner"].apply(_normalize_bp_value)
    _log_debug(f"Infos file loaded: {infos_path} ({len(infos)} rows)")
    
    print(infos.describe())
    df = _coerce_id_columns(df)
    if "BP" in df.columns:
        df["BP"] = df["BP"].apply(_normalize_bp_value)
    df = mt.merge_df(df, infos)
    _log_debug(f"Merged base dataframe with infos : \n{df.describe()}")
    
    join_table = pd.read_csv(
        join_table_path,
        sep=";",
        engine="python",
        quoting=csv.QUOTE_NONE,
        on_bad_lines="skip",
        dtype=str,
        names=["Business Partner", "Addr. No."],
    ).astype(str)
    join_table = _coerce_id_columns(join_table)
    join_table = join_table[["Business Partner", "Addr. No."]]
    join_table = join_table.sort_values(by=["Business Partner", "Addr. No."], ascending=[True, False])
    join_table = join_table.drop_duplicates(subset=["Business Partner"])
    address_table = pd.read_csv(
        address_table_path,
        sep=";",
        engine="python",
        quoting=csv.QUOTE_NONE,
        on_bad_lines="skip",
        dtype=str,
    )
    address_table = _coerce_id_columns(address_table)
    if address_table.empty:
        _log_warn(f"Address table empty or unreadable: {address_table_path}")
    elif len(address_table.columns) < 30:
        _log_warn(f"Address table has unexpected format ({len(address_table.columns)} columns), skipping address merge.")
        address_table = pd.DataFrame(columns=["Addr. No."])
    else:
        rename_map = {
            address_table.columns[0]: "Addr. No.",
            address_table.columns[26]: "street",
            address_table.columns[29]: "street4",
            address_table.columns[20]: "street5",
            address_table.columns[5]: "city",
            address_table.columns[4]: "postcode",
            address_table.columns[11]: "country",
        }
        address_table.rename(columns=rename_map, inplace=True)
        keep_columns = [col for col in ("Addr. No.", "street", "street4", "street5", "city", "postcode", "country") if col in address_table.columns]
        address_table = address_table[keep_columns]
        missing_cols = set(["street", "street4", "street5", "city", "postcode", "country"]) - set(address_table.columns)
        if missing_cols:
            _log_warn(f"Missing address columns: {sorted(missing_cols)}")
    
    
    address_table = pd.merge(left=join_table, right=address_table, on="Addr. No.", how="left")
    
    
    
    # Normaliser les types pour la jointure adresse.
    if not address_table.empty:
        address_table.iloc[:, 0] = address_table.iloc[:, 0].astype(str)
    done = []
    n_df = len(df)

    update_status("Building partner dataset...")
    _log_info(f"Building partner dataset: {n_df} input rows, output -> {output_path}")
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
        for i, row in df.iterrows():
            partner = row.get("BP")

            if update_status:
                update_status(f"Partenaire {i+1}/{n_df} : {partner}")
            _log_debug(f"Processing partner {i+1}/{n_df}: {partner}")

            if partner not in done:
                # Conserver les colonnes du merge (nom, pays...) et y ajouter les infos SIREN/SIRET calculées
                part_data = tp.main(partner, df)
                merged_row = row.to_dict()
                merged_row.update(part_data)
                newline = pd.DataFrame([merged_row]).astype(str)
                # newline = merge_address(newline, join_table, address_table)
                output = pd.concat([output, newline], ignore_index=True)

                output.tail(1).to_excel(
                    writer,
                    index=False,
                    header=False,
                    startrow=writer.sheets["Sheet1"].max_row
                )
                _log_debug(f"Wrote partner row {i+1} to {output_path}")

            done.append(partner)

    
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
