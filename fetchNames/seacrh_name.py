import requests
import re

URL = "https://www.info-clipper.com/fr/index.php"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://www.info-clipper.com",
    "Referer": "",
}

REFER_DE = "https://www.info-clipper.com/fr/entreprise/recherche/allemagne.de.html"
REFER_ES = "https://www.info-clipper.com/fr/entreprise/recherche/espagne.es.html"

def get_name_from_nif(nif, country_code):
    # ✅ Dans le HAR: POST sur index.php?q=SearchItems&country_code=ES
    params = {"q": "SearchItems", "country_code": country_code}

    # ✅ Dans le HAR: my_bizid=nif (le NIF/CIF est ici)
    data = {
        "e": "",
        "my_search": "",
        "my_town": "",
        "my_duns": "",
        "my_tel": "",
        "my_bizid": nif,
        "searchLocal": "",
    }
    
    headers = HEADERS.copy()
    headers["Referer"] = REFER_ES if country_code == "ES" else REFER_DE

    r = requests.post(URL, params=params, data=data, headers=headers, timeout=20)
    r.raise_for_status()
    html = r.text

    # ✅ Les noms sont dans: <span class='titre'>NOM</span>
    names = re.findall(r"<span\s+class=['\"]titre['\"]>\s*([^<]+?)\s*</span>", html, flags=re.IGNORECASE)

    # Nettoyage léger
    names = [re.sub(r"\s+", " ", n).strip() for n in names]

    if not names:
        return None

    return names[0]


if __name__ == "__main__":
    nif = "A50996933"  # 👈 modifie ici
    name = get_name_from_nif(nif)

    if name:
        print(f"{nif} → {name}")
    else:
        print(f"{nif} → Entreprise non trouvée (ou blocage HTML)")
