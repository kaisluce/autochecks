import os
import sys
import pandas as pd
import forSirenSiret.treatpartner as tp
import forSirenSiret.merge_tables as mt


def build_partner_dataset(df: pd.DataFrame, infos_path: str, output_dir: str, update_status=None):
    """
    Construit un jeu de données consolidé pour les partenaires en traitant un DataFrame d'entrée,
    puis en le fusionnant avec des informations supplémentaires.
    """
    if update_status is None:
        update_status = lambda msg: None

    def log(msg: str):
        print(msg, file=sys.stdout, flush=True)

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "latest_datas.xlsx")

    output = pd.DataFrame().astype(str)
    output.to_excel(output_path, index=False)

    done = []
    n_df = len(df)

    update_status("Building partner dataset...")
    log(f"[INFO] Building partner dataset: {n_df} input rows, output -> {output_path}")
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
        for i, row in df.iterrows():
            partner = row.get("BP")

            if update_status:
                update_status(f"Partenaire {i+1}/{n_df} : {partner}")
            log(f"[INFO] Processing partner {i+1}/{n_df}: {partner}")

            if partner not in done:
                part_data = tp.main(partner, df)
                newline = pd.DataFrame([part_data]).astype(str)
                output = pd.concat([output, newline], ignore_index=True)

                output.tail(1).to_excel(
                    writer,
                    index=False,
                    header=False,
                    startrow=writer.sheets["Sheet1"].max_row
                )
                log(f"[INFO] Wrote partner row {i+1} to {output_path}")

            done.append(partner)

    # Lecture du fichier infos (CSV séparé par ;), en ignorant les lignes malformées.
    infos = pd.read_csv(
        infos_path,
        sep=";",
        dtype=str,
        on_bad_lines="skip",
        engine="python",
    ).astype(str)

    # Harmoniser la clé de jointure tout en conservant 'partner' pour le report.
    output["BP"] = output.get("partner", output.get("BP"))
    output = mt.merge_df(output, infos)
    
    # Filtre optionnel : exclure les lignes dont le nom contient "snetor".
    name_col = "Name 1" if "Name 1" in output.columns else "Name1"
    if name_col in output.columns:
        mask1 = ~output[name_col].str.lower().fillna("").str.contains("snetor")
        mask2 = ~output[name_col].str.lower().fillna("").str.contains("gazechim")
        mask3 = ~output[name_col].str.upper().fillna("").str.contains("OZYANCE")
        output = output.loc[mask1]
        output = output.loc[mask2]
        output = output.loc[mask3]

    output.to_excel(output_path, index=False)
    log(f"[INFO] Final partner dataset saved: {output_path} ({len(output)} rows)")

    return output, output_path
