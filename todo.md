# TODO

- Corriger l'appel de `vat_process.main` (signature/kwargs) et gérer les erreurs des threads {cm:2026-02-05} {h}
- Remplacer le `while True` + `except Exception` de `forVats/batchFile.py` par des retries bornés et des exceptions ciblées {cm:2026-02-05} {h}
- Protéger les appels réseau dans `forVats/checkcomplete.py` (`get_status` / download report) pour éviter l'arrêt global {cm:2026-02-05} {h}
- Ajouter des `timeout` explicites à toutes les requêtes HTTP VAT (`forceHTTP.py`, `get_status.py`, `downloadrepport.py`) {cm:2026-02-05} {h}
- Remplacer les `raise exc` par `raise` dans `forVats/process.py` et `forSirenSiret/checks.py` {cm:2026-02-05} {h}
- Gérer les erreurs HTTP/réseau dans le flux `fetchNames` pour ne pas casser tout le traitement {cm:2026-02-05} {h}
- Améliorer la remontée d'erreur finale de `handcheck.py` (garder traceback exploitable) {cm:2026-02-05} {h}

vérifier que les mails c bon {cm:2026-02-16}
mettre a jour drom et COM