# MDM Autochecks

Pipeline sans interface qui consolide les identifiants Business Partner, vérifie les SIREN/SIRET via l'API SIRENE de l'INSEE, valide les numéros de TVA via le service batch VIES de l'UE, puis envoie par mail les rapports Excel d'anomalies.

## Fonctionnalités

- Un lancement exécute les deux flux en parallèle : vérification SIREN/SIRET et validation TVA batch.
- Construit un jeu de données partenaire propre à partir des exports SAP (pivot des codes FR0/FR1/FR2, normalisation des identifiants, fusion des tables noms et adresses) dans `latest_datas.xlsx`.
- Les appels SIRENE aplatissent les données d'unité légale et d'établissement, ajoutent les indicateurs de doublons et d'incohérences, puis écrivent un `latest_report.xlsx` colorisé.
- Le flux TVA reformate les valeurs en lots de `50`, les envoie à VIES, suit les tokens, télécharge les fichiers Excel par lot, les concatène, puis rattache les numéros de BP.
- Le récupérateur de noms pour les TVA allemandes et espagnoles appelle l'API de recherche TVA, reconstruit `Fetched Name` et compare avec les noms SAP pour signaler les écarts.
- L'étape email (librairie personnalisée mails) envoie des extractions ciblées : SIRET/SIREN inactifs, doublons de SIRET, problèmes de noms, TVA invalides.
- Journalisation : chaque exécution écrit un fichier de log horodaté dans `logs/` (issu de ma librairie personnélisée logger), avec la progression, les avertissements (anomalies d'entrée, fallbacks) et les erreurs.

## Fonctionnement du pipeline

1) **Chargement des entrées** : `.env` fournit les chemins. `main.py` lit le fichier brut des numéros fiscaux (`INPUT_FILE` dans `INPUTS`), conserve les colonnes `[BP, value, type]`, pivote FR0 -> `VAT`, FR1 -> `siret`, FR2 -> `siren`, et remappe les codes TVA UE se terminant par `0` vers FR0.
2) **Enrichissement des partenaires** (`forSirenSiret.partner_processing.build_partner_dataset`) : normalise les identifiants, fusionne le fichier des noms (`NAMES_FILE`), joint les données d'adresse (`JOIN_TABLE`, `ADRESS_TABLE`), détecte les doublons / longueurs invalides et écrit `latest_datas.xlsx` dans le dossier de sortie horodaté.
3) **Contrôles SIREN/SIRET** (`forSirenSiret.checks.generate_report`) : choisit SIREN, SIRET ou les deux selon la cohérence des données, relance en cas d'erreur réseau, puis ajoute les lignes avec formatage conditionnel dans `siren_siret/latest_report.xlsx`.
4) **Contrôles TVA** (`forVats.process.process`) : découpe la colonne `VAT` en CSV par lots de `50`, soumet à VIES, interroge les tokens jusqu'à ce que son status soit "`COMPLETED`", télécharge chaque rapport, concatène le tout dans `vat/report_concatenated.xlsx` et rattache les IDs BP.
5) **Exports mail** (`emailing/`) : construit les extractions ciblées (`closed_siret.xlsx`, `closed_siren.xlsx`, `duplicated_siret.xlsx`, `wrong_name.xlsx`, `bad_vats.xlsx`) et les envoie depuis la boîte partagée via la librairie mails.

## Entrées attendues

- `.env` à la racine du dépôt :
  - `DIRECTORY_LOCATION` : dossier racine où sont créés les répertoires d'exécution horodatés.
  - `INPUTS` : dossier contenant les CSV bruts.
  - `INPUT_FILE` : export des numéros fiscaux (ex. `BP_TAXNUM.csv`, colonnes : id BP, valeur, code de type comme FR0/FR1/FR2 ou codes TVA UE finissant par `0`).
  - `NAMES_FILE` : export des infos BP (ex. `BP_BUT000.csv`, doit contenir `Business Partner`, `Name`, `Country/Region Key`, etc.).
  - `JOIN_TABLE` : mapping BP -> id d'adresse (ex. `BP_BUT020.csv`).
  - `ADRESS_TABLE` : table des adresses (ex. `BP_ADRC.csv`).

### Format d'entrée (CSV SAP)

Formats typiquement lus par `main.py` / `handcheck.py` :

- Sans en-têtes (séparateur `;`) :

  ```
  BP      ; value        ; type ; UNKNOWN TEXT
  900080  ; FR25784158545; FR0  ; 0
  900080  ; 7,84E+08     ; FR2  ; 0
  900080  ; 7,84E+13     ; FR1  ; 0
  ```

- Avec en-têtes français (détection automatique du séparateur, colonnes du type `Partenaire`, `Cat. N° ID fiscale`, `N° ID fiscale`, `N° ID fiscale (long)`, ...) :

  ```
  Partenaire,Cat. N° ID fiscale,N° ID fiscale,N° ID fiscale (long),...
  1000915,FR0,FR95790959096,,
  1000915,FR1,7909950900019,,
  1000915,FR2,790995096,,
  ```

Seules les 3 premières colonnes sont utilisées puis renommées en `BP`, `type`, `value`. Les codes UE finissant par `0` sont remappés en `FR0` pour alimenter la colonne `VAT`.

## Sorties

- `<DIRECTORY_LOCATION>/<timestamp>/latest_datas.xlsx` : lignes partenaires consolidées avec identifiants normalisés, doublons, indicateurs d'adresse et métadonnées BP.
- `.../siren_siret/latest_report.xlsx` : résultats API SIREN/SIRET.
- `.../vat/report_concatenated.xlsx` : résultats VIES fusionnés avec les noms de fichiers source et le rattachement BP.
- `.../fetchedNames.xlsx` : récupération des noms pour TVA allemandes / espagnoles avec `Fetched Name`, `SAP Name`, diagnostic de correspondance et scores.
- `.../wrong_name.xlsx` : sous-ensemble de `fetchedNames.xlsx` où `name match diag` n'est ni `exact`, ni `Name not fetched`, ni `Missing name`.
- `.../siren_siret/closed_siret.xlsx`, `.../siren_siret/closed_siren.xlsx`, `.../siren_siret/duplicated_siret.xlsx`, `.../vat/bad_vats.xlsx` : extractions d'anomalies utilisées par l'envoi mail.

### Structure des fichiers de sortie (colonnes principales)

- `latest_datas.xlsx` (Sheet1) :
  - Identifiants : `BP`, `siren`, `siret`, `VAT`
  - Indicateurs : `duplicates_siren`, `duplicates_siret`, `duplicates_VAT`, `missing siren`, `missing siret`, `Missing_Vat`, `Missmatching siren siret`, `Missmatching siren VAT`, `uses a snetor *`
  - Infos BP / adresse : `Name 1`, `Grouping`, `Country/Region Key`, `Language Key`, `adressID`, `street`, `street4`, `street5`, `city`, `postcode`, `country`, `has_*`
- `siren_siret/latest_report.xlsx` (feuille `Report`) :
  - Identifiants : `BP`, `type` (siren/siret), `siret`, `nic`, `siren`
  - Données INSEE : `status`, `siege`, `denomination`, `date_creation`, `date_cessation`, `naf`, `naf_label`, `cat_juridique`, `adresse`, `n_voie`, `voie`, `code_postal`, `commune`, `siret_siege`
  - Contrôles locaux : `duplicates_*`, `missing *`, `uses a snetor *`, `Missmatching siren *`
  - Synthèse : `report`
- `vat/report_concatenated.xlsx` :
  - Colonnes VIES : `MS Code`, `VAT Number`, `Requester MS Code`, `Requester VAT Number`, champs de validité / statut VIES
  - Ajouts locaux : `__source_file__` (lot d'origine), `BP` (liste des BPs correspondant à la TVA)

## Arborescence du dépôt

```
autochecks/
|-- main.py
|-- handcheck.py
|-- handcheck.spec
|-- README.md
|-- requirements.txt
|-- emailing/
|   |-- mail_export.py
|   |-- siren_mail.py
|   |-- vat_mail.py
|   `-- logs/
|-- fetchNames/
|   |-- compare_names.py
|   |-- get_names_from_last_report.py
|   `-- seacrh_name.py
|-- forSirenSiret/
|   |-- checks.py
|   |-- merge_tables.py
|   |-- partner_processing.py
|   |-- requestsiren.py
|   |-- requestsiret.py
|   `-- treatpartner.py
|-- forVats/
|   |-- batchFile.py
|   |-- checkcomplete.py
|   |-- concat.py
|   |-- downloadrepport.py
|   |-- forceHTTP.py
|   |-- get_status.py
|   |-- multibash.py
|   |-- process.py
|   |-- rebuild.py
|   `-- reformate.py
`-- logs/
```

## Arborescence des sorties

```
YYYY-MM-DD_HH-MM_REPORT/
|-- latest_datas.xlsx            # partenaires consolidés
|-- fetchedNames.xlsx            # noms DE/ES récupérés + comparaison SAP
|-- wrong_name.xlsx              # sous-ensemble des écarts de noms
|-- siren_siret/
|   |-- latest_report.xlsx       # résultats INSEE avec formatage conditionnel
|   |-- closed_siret.xlsx
|   |-- closed_siren.xlsx
|   `-- duplicated_siret.xlsx
`-- vat/
    |-- data/                    # CSV batchs envoyés à VIES
    |-- reports/                 # rapports Excel VIES téléchargés
    |-- report_concatenated.xlsx # fusion des rapports VIES
    |-- bad_vats.xlsx
    `-- tokens.csv               # mapping batch_file -> token
```

## Résumé du workflow

1) Sélectionner le CSV SAP (ou lancer `main.py` pour prendre `INPUT_FILE` depuis `.env`).
2) Le script pivote FR0/FR1/FR2 (et les codes UE `*0`) vers `VAT` / `siret` / `siren`, normalise puis enrichit.
3) Lancement parallèle des contrôles SIRENE (SIREN/SIRET) et VIES (TVA).
4) Les sorties Excel sont écrites dans le dossier daté (`..._REPORT` ou `..._HANDCHECK_REPORT`), puis les exports mails sont générés.

## Résultat attendu (exemple)

```
2025-12-22_11-57_REPORT/
|-- latest_datas.xlsx
|-- fetchedNames.xlsx
|-- wrong_name.xlsx
|-- siren_siret/
|   |-- latest_report.xlsx
|   |-- duplicated_siret.xlsx
|   |-- closed_siret.xlsx
|   `-- closed_siren.xlsx
`-- vat/
    |-- data/
    |   |-- BP_TAXNUM_part000.csv
    |   |-- BP_TAXNUM_part001.csv
    |   `-- ...
    |-- reports/
    |   |-- BP_TAXNUM_part000_report.xlsx
    |   |-- BP_TAXNUM_part001_report.xlsx
    |   `-- ...
    |-- report_concatenated.xlsx
    |-- bad_vats.xlsx
    `-- tokens.csv
```

## Endpoints API utilisés

- INSEE SIRENE : `GET https://api-avis-situation-sirene.insee.fr/identification/siren/{siren}?telephone=` et `GET https://api-avis-situation-sirene.insee.fr/identification/siret/{siret}?telephone=`.
- VIES batch TVA UE : `POST https://ec.europa.eu/taxation_customs/vies/rest-api/vat-validation` (upload CSV), `GET https://ec.europa.eu/taxation_customs/vies/rest-api/vat-validation/{token}` (statut), `GET https://ec.europa.eu/taxation_customs/vies/rest-api/vat-validation-report/{token}` (rapport Excel).

## Exécuter le batch

1. Installer les dépendances (de préférence dans un venv) :

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Configurer `.env` avec les bons chemins mis à jour.
3. Lancer :

   ```powershell
   python main.py
   ```

   Cela crée un dossier horodaté sous `DIRECTORY_LOCATION` avec les sous-dossiers `siren_siret` et `vat`. Un accès réseau est nécessaire pour les appels INSEE et VIES.
