# TODO

- Corriger l'appel de `vat_process.main` (signature/kwargs) et gérer les erreurs des threads {cm:2026-02-05} {h}
- Remplacer le `while True` + `except Exception` de `forVats/batchFile.py` par des retries bornés et des exceptions ciblées {cm:2026-02-05} {h}
- Protéger les appels réseau dans `forVats/checkcomplete.py` (`get_status` / download report) pour éviter l'arrêt global {cm:2026-02-05} {h}
- Ajouter des `timeout` explicites à toutes les requêtes HTTP VAT (`forceHTTP.py`, `get_status.py`, `downloadrepport.py`) {cm:2026-02-05} {h}
- Remplacer les `raise exc` par `raise` dans `forVats/process.py` et `forSirenSiret/checks.py` {cm:2026-02-05} {h}
- Gérer les erreurs HTTP/réseau dans le flux `fetchNames` pour ne pas casser tout le traitement {cm:2026-02-05} {h}
- Améliorer la remontée d'erreur finale de `handcheck.py` (garder traceback exploitable) {cm:2026-02-05} {h}

Missing ZLOT {cm:2026-03-16} {h} {h}
Missing ZLOT prod {cm:2026-03-16} {h}
payements
street drop 10000010 et 12 {cm:2026-03-17} {h}
comprendre ce qu'elle veut dire par histoire d'heure {cm:2026-03-17} {h}
caractere speciaux gpm assignation {cm:2026-03-17} {h}
product hierarchy a expliquer pour la Nième fois {cm:2026-03-17}
street quand Name 1 vide, chercher la langue dans correspondance language {cm:2026-03-17} {h}
sales users, demander c quoi le pb
orthographe hscodes(avaliable) {cm:2026-03-17} {h}
concat last et first name dans les cas ou name est vide {cm:2026-03-17} {h}
renommer customer vue credit vue {cm:2026-03-17}
renommer diag hscodes (incorrect input = format issue, col name = present in tables {cm:2026-03-17}
prod hierarchy consistency
prod new hierarchy
prod credit vue
prod street language