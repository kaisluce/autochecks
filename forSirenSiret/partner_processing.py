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
    # Keep only digits for padding; if no digits remain, fall back to the trimmed string.
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        return digits.zfill(10)
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
    adress_table_path: str,
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

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "latest_datas.xlsx")

    output = pd.DataFrame().astype(str)
    output.to_excel(output_path, index=False)
    _log_debug(f"Initialized output file at {output_path}")

    # Lecture du fichier infos (CSV séparé par ;), en ignorant les lignes malformées.
    infos = pd.read_csv(
        infos_path,
        sep=";",
        dtype=str,
        on_bad_lines="skip",
        engine="python",
    ).astype(str)
    infos = _coerce_id_columns(infos)
    _log_debug(f"Infos file loaded: {infos_path} ({len(infos)} rows)")
    
    
    df = _coerce_id_columns(df)
    df = mt.merge_df(df, infos)
    _log_debug("Merged base dataframe with infos")
    
    join_table = pd.read_csv(
        join_table_path,
        sep=";",
        engine="python",
        quoting=csv.QUOTE_NONE,
        on_bad_lines="skip",
        dtype=str,
    ).astype(str)
    join_table = _coerce_id_columns(join_table)
    join_table = join_table[["Business Partner", "Addr. No."]]
    join_table = join_table.sort_values(by=["Business Partner", "Addr. No."], ascending=[True, False])
    join_table = join_table.drop_duplicates(subset=["Business Partner"])
    adress_table = pd.read_csv(
        adress_table_path,
        sep=";",
        engine="python",
        quoting=csv.QUOTE_NONE,
        on_bad_lines="skip",
        dtype=str,
    )
    adress_table = _coerce_id_columns(adress_table)
    if adress_table.empty:
        _log_warn(f"Address table empty or unreadable: {adress_table_path}")
    elif len(adress_table.columns) < 30:
        _log_warn(f"Address table has unexpected format ({len(adress_table.columns)} columns), skipping address merge.")
        adress_table = pd.DataFrame(columns=["Addr. No."])
    else:
        rename_map = {
            adress_table.columns[0]: "Addr. No.",
            adress_table.columns[26]: "street",
            adress_table.columns[29]: "street4",
            adress_table.columns[20]: "street5",
            adress_table.columns[5]: "city",
            adress_table.columns[4]: "postcode",
            adress_table.columns[11]: "country",
        }
        adress_table.rename(columns=rename_map, inplace=True)
        keep_columns = [col for col in ("Addr. No.", "street", "street4", "street5", "city", "postcode", "country") if col in adress_table.columns]
        adress_table = adress_table[keep_columns]
        missing_cols = set(["street", "street4", "street5", "city", "postcode", "country"]) - set(adress_table.columns)
        if missing_cols:
            _log_warn(f"Missing address columns: {sorted(missing_cols)}")
    
    
    adress_table = pd.merge(left=join_table, right=adress_table, on="Addr. No.", how="outer")
    
    
    
    # Normaliser les types pour la jointure adresse.
    if not adress_table.empty:
        adress_table.iloc[:, 0] = adress_table.iloc[:, 0].astype(str)
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
                # newline = merge_address(newline, join_table, adress_table)
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
            "Grp.",
            "Search Term 1",
            "Search Term 2",
            "External BP Number",
            "BPC",
            "Last name",
            "First name",
            "Unnamed: 19"
        ],
        errors="ignore")
    check_cols = [col for col in output.columns if col != "BP"]
    adress_table.rename(columns={"Business Partner": "BP"}, inplace=True)
    output = pd.merge(left=output, right=adress_table, on="BP", how="outer")
    output = output.dropna(subset=check_cols, how="all")
    
    output.to_excel(output_path, index=False)
    _log_info(f"Final partner dataset saved: {output_path} ({len(output)} rows)")

    return output, output_path

# Redéfinition de merge_address pour traiter tout un DataFrame et corriger l'application au niveau des lignes.
def merge_address(datas: pd.DataFrame, join_table: pd.DataFrame, adress_table: pd.DataFrame):
    """
    Complète les champs d'adresse pour chaque ligne du DataFrame.
    Sélectionne le plus grand `adress_ID` pour un BP donné et renseigne les flags `has_*`
    en excluant les valeurs vides ou placeholders.
    """
    
    print(f"[DEBUG] Processing BP={datas.iloc[0].get('partner', '')}")
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


    addr_key = adress_table.columns[0] if (not adress_table.empty and len(adress_table.columns) > 0) else None
    if addr_key is not None:
        adress_table[addr_key] = adress_table[addr_key].astype(str)

    def _fill_row(row: pd.Series) -> pd.Series:
        print(f"[DEBUG] Processing BP={row.get('partner', '')}")
        bp_val = str(row.get("partner", ""))
        matches = join_table[join_table["Business Partner"] == bp_val]
        print(f"[DEBUG]   Matches for BP={bp_val}: {len(matches)}")

        # valeurs par défaut
        row["adressID"] = ""
        row["street"] = ""
        row["street4"] = ""
        row["street5"] = ""
        row["city"] = ""
        row["postcode"] = ""
        row["has_addressID"] = False
        row["has_street"] = False
        row["has_street4"] = False
        row["has_street5"] = False
        row["has_city"] = False
        row["has_postcode"] = False

        if matches.empty or addr_key is None:
            print(f"[DEBUG]   No matches in join_table or addr_key missing for BP={bp_val}")
            return row

        adress_id = matches["Addr. No."].max()
        row["adressID"] = adress_id
        row["has_addressID"] = _is_valid(adress_id)
        if not row["has_addressID"]:
            print(f"[DEBUG]   Invalid adress_ID={adress_id} for BP={bp_val}")
            return row

        address_match = adress_table[adress_table[addr_key] == str(adress_id)]
        if address_match.empty:
            print(f"[DEBUG]   No address row for adress_ID={adress_id} (BP={bp_val})")
            return row

        adress = address_match.iloc[0]

        street = adress.get("street")
        street4 = adress.get("street4")
        street5 = adress.get("street5")
        city = adress.get("city")
        postcode = adress.get("postcode")
        country = adress.get("country")

        print(f"[DEBUG]   Fields for BP={bp_val} / adress_ID={adress_id}: street={street} street4={street4} street5={street5} city={city} postcode={postcode}")

        row["street"] = street
        row["street4"] = street4
        row["street5"] = street5
        row["city"] = city
        row["postcode"] = postcode
        row["country"] = country

        row["has_street"] = _is_valid(street)
        row["has_street4"] = _is_valid(street4)
        row["has_street5"] = _is_valid(street5)
        row["has_city"] = _is_valid(city)
        row["has_postcode"] = _is_valid_postcode(postcode)

        return row

    return datas.apply(_fill_row, axis=1)
