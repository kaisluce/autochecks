import re
import time

import requests
from requests.exceptions import RequestException, SSLError

URL = "https://www.info-clipper.com/fr/index.php"
INSECURE_SSL_VERIFY = False

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


def _resolve_verify():
    return INSECURE_SSL_VERIFY


def get_name_from_nif(nif, country_code, timeout=20, max_retries=3, retry_delay=2):
    if not nif or not country_code:
        return None

    params = {"q": "SearchItems", "country_code": country_code}
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
    verify = _resolve_verify()

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                URL,
                params=params,
                data=data,
                headers=headers,
                timeout=timeout,
                verify=verify,
            )
            response.raise_for_status()
            html = response.text

            names = re.findall(r"<span\s+class=['\"]titre['\"]>\s*([^<]+?)\s*</span>", html, flags=re.IGNORECASE)
            names = [re.sub(r"\s+", " ", n).strip() for n in names]
            return names[0] if names else None
        except SSLError as exc:
            last_exc = exc
            if attempt < max_retries:
                time.sleep(retry_delay)
                continue
            break
        except RequestException as exc:
            last_exc = exc
            if attempt < max_retries:
                time.sleep(retry_delay)
                continue
            break

    hint = ""
    if isinstance(last_exc, SSLError):
        hint = " SSL verify failed even with verify=False; check proxy/network interception rules."
    raise RuntimeError(
        f"Name lookup failed for NIF={nif} country={country_code} after {max_retries} attempts: {last_exc}.{hint}"
    ) from last_exc


if __name__ == "__main__":
    nif = "DE230225490"
    name = get_name_from_nif(nif, "DE")

    if name:
        print(f"{nif} -> {name}")
    else:
        print(f"{nif} -> Entreprise non trouvee (ou blocage HTML)")
