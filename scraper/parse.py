from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


EURO = "\u20ac"

CURRENCY_MAP = {
    "Ft": "HUF",
    "HUF": "HUF",
    "EUR": "EUR",
    EURO: "EUR",
}

CATEGORY_PATTERNS = [
    "szemelyauto",
    "teherauto",
    "motor",
    "lakoauto",
    "autobusz",
    "mikrobusz",
    "kamion",
    "potkocsi",
]

LABEL_MAP = {
    "Evjarat": "year_month",
    "Evjarat (gyartasi ev)": "year_month",
    "Km. ora allas": "mileage_km",
    "Uzemanyag": "fuel",
    "Hengerurtartalom": "engine_cc",
    "Teljesitmeny": "power",
    "Sebessegvalto": "transmission",
    "Hajtas": "drivetrain",
    "Kivitel": "body_type",
    "Allapot": "condition",
    "Szin": "color",
    "Ajtok szama": "doors",
    "Szallithato szem. szama": "seats",
}

ACCENT_MAP = str.maketrans(
    {
        "\u00e1": "a",  # a acute
        "\u00c1": "A",
        "\u00e9": "e",  # e acute
        "\u00c9": "E",
        "\u00ed": "i",  # i acute
        "\u00cd": "I",
        "\u00f3": "o",  # o acute
        "\u00d3": "O",
        "\u00f6": "o",  # o umlaut
        "\u00d6": "O",
        "\u0151": "o",  # o double acute
        "\u0150": "O",
        "\u00fa": "u",  # u acute
        "\u00da": "U",
        "\u00fc": "u",  # u umlaut
        "\u00dc": "U",
        "\u0171": "u",  # u double acute
        "\u0170": "U",
    }
)


def strip_accents(text: str) -> str:
    return text.translate(ACCENT_MAP)


def normalize_label(text: str) -> str:
    return strip_accents(text.strip())


def parse_int(text: str) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_price_amount(text: str) -> Optional[int]:
    return parse_int(text)


def dedupe_urls(items: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
    seen = set()
    unique: List[Tuple[str, int]] = []
    for url, ad_id in items:
        key = (url, ad_id)
        if key in seen:
            continue
        seen.add(key)
        unique.append((url, ad_id))
    return unique


def extract_listing_urls(html: str, base_url: str, categories: Optional[List[str]] = None) -> List[Tuple[str, int]]:
    soup = BeautifulSoup(html, "lxml")
    found: List[Tuple[str, int]] = []
    allowed_categories = set(categories or CATEGORY_PATTERNS)

    for link in soup.find_all("a", href=True):
        href = link.get("href", "").strip()
        if not href:
            continue
        if href.startswith("//"):
            href = "https:" + href
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if "hasznaltauto.hu" not in parsed.netloc:
            continue

        for category in allowed_categories:
            match = re.search(rf"/{category}/.+-(\d+)(?:/|$|\\.html$)", parsed.path)
            if not match:
                match = re.search(rf"/{category}/.+/(\d+)(?:/|$)", parsed.path)
            if match:
                ad_id = int(match.group(1))
                found.append((full_url, ad_id))
                break

    return dedupe_urls(found)


def extract_kv_pairs(soup: BeautifulSoup) -> Dict[str, str]:
    kv: Dict[str, str] = {}

    def add_pair(key: str, val: str) -> None:
        if not key or not val:
            return
        kv.setdefault(key, val)
        key_ascii = strip_accents(key)
        if key_ascii != key:
            kv.setdefault(key_ascii, val)

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in row.find_all(["th", "td"])]
            if len(cells) >= 2:
                add_pair(cells[0], cells[1])

    for dl in soup.find_all("dl"):
        terms = dl.find_all("dt")
        defs = dl.find_all("dd")
        for term, definition in zip(terms, defs):
            add_pair(term.get_text(" ", strip=True), definition.get_text(" ", strip=True))

    # Fallback: scan sequential text lines
    text_lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
    if text_lines:
        for idx, line in enumerate(text_lines[:-1]):
            if line in kv:
                continue
            next_line = text_lines[idx + 1]
            if len(line) < 64 and len(next_line) < 128:
                if ":" not in line and line.isupper() is False:
                    line_ascii = strip_accents(line)
                    if line_ascii.lower().startswith("hirdeteskod"):
                        add_pair("Hirdeteskod", next_line)
                    if line_ascii in (
                        "Evjarat",
                        "Km. ora allas",
                        "Uzemanyag",
                        "Hengerurtartalom",
                        "Teljesitmeny",
                    ):
                        add_pair(line_ascii, next_line)

    return kv


def extract_meta(soup: BeautifulSoup, name: str, attr: str = "property") -> Optional[str]:
    tag = soup.find("meta", attrs={attr: name})
    if tag and tag.get("content"):
        return tag.get("content").strip()
    return None


def extract_title(soup: BeautifulSoup) -> Optional[str]:
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(" ", strip=True)
        if text:
            return text
    return extract_meta(soup, "og:title")


def _find_heading(soup: BeautifulSoup, label_ascii: str):
    pattern = re.compile(re.escape(label_ascii), re.IGNORECASE)

    def matcher(text: str) -> bool:
        if not text:
            return False
        return bool(pattern.search(strip_accents(text)))

    return soup.find(string=matcher)


def extract_description(soup: BeautifulSoup) -> Optional[str]:
    heading = _find_heading(soup, "Leiras")
    if heading:
        container = heading.find_parent()
        if container:
            text = container.get_text(" ", strip=True)
            if text and len(text) > 20:
                return text
    return extract_meta(soup, "og:description")


def extract_images(soup: BeautifulSoup, base_url: str) -> List[str]:
    images: List[str] = []
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src")
        if not src:
            continue
        src = src.strip()
        if src.startswith("//"):
            src = "https:" + src
        src = urljoin(base_url, src)
        if "hasznaltauto.hu" not in src:
            continue
        if src not in images:
            images.append(src)
    return images


def extract_equipment(soup: BeautifulSoup) -> List[str]:
    equipment: List[str] = []
    heading = _find_heading(soup, "Felszereltseg")
    if heading:
        container = heading.find_parent()
        if container:
            for li in container.find_all("li"):
                text = li.get_text(" ", strip=True)
                if text and text not in equipment:
                    equipment.append(text)

    if not equipment:
        for node in soup.find_all(["section", "div"], attrs={"id": re.compile("felszerelt", re.I)}):
            for li in node.find_all("li"):
                text = li.get_text(" ", strip=True)
                if text and text not in equipment:
                    equipment.append(text)

    return equipment


def parse_detail(html: str, url: str, base_url: str) -> Dict[str, object]:
    soup = BeautifulSoup(html, "lxml")

    data: Dict[str, object] = {
        "url": url,
        "title": extract_title(soup),
        "description": extract_description(soup),
        "images": extract_images(soup, base_url),
        "equipment": extract_equipment(soup),
    }

    kv = extract_kv_pairs(soup)
    data["attributes"] = kv

    # Ad ID
    ad_id = None
    for key in ("Hirdeteskod", "Hirdetes kod", "HirdetesKod"):
        if key in kv:
            ad_id = parse_int(kv[key])
            break
    if ad_id is None:
        match = re.search(r"-(\d+)$", url)
        if match:
            ad_id = int(match.group(1))
    data["ad_id"] = ad_id

    # Price extraction
    price = None
    price_discount = None
    currency = None

    for meta_name in ("product:price:amount", "og:price:amount"):
        meta_val = extract_meta(soup, meta_name)
        if meta_val:
            price = parse_price_amount(meta_val)
    currency_meta = extract_meta(soup, "product:price:currency")
    if currency_meta:
        currency = currency_meta

    if price is None:
        text = soup.get_text("\n", strip=True)
        for line in text.splitlines():
            line_ascii = strip_accents(line)
            if "Akcio" in line_ascii:
                match = re.search(r"([0-9][0-9\s\xa0\.]*)\s*(Ft|HUF|EUR|\u20ac)", line)
                if match:
                    price_discount = parse_price_amount(match.group(1))
                    currency = CURRENCY_MAP.get(match.group(2), currency)
            if price is None:
                match = re.search(r"([0-9][0-9\s\xa0\.]*)\s*(Ft|HUF|EUR|\u20ac)", line)
                if match:
                    price = parse_price_amount(match.group(1))
                    currency = CURRENCY_MAP.get(match.group(2), currency)

    data["price_huf"] = price
    data["price_discount_huf"] = price_discount
    data["currency"] = currency

    # Normalize key attributes
    for raw_key, value in kv.items():
        normalized = normalize_label(raw_key)
        mapped = LABEL_MAP.get(normalized) or LABEL_MAP.get(raw_key)
        if not mapped:
            continue
        if mapped == "year_month":
            data["year_month"] = value
            data["year"] = parse_int(value)
        elif mapped == "mileage_km":
            data["mileage_km"] = parse_int(value)
        elif mapped == "engine_cc":
            data["engine_cc"] = parse_int(value)
        elif mapped == "power":
            # often like "66 kW, 90 LE"
            match_kw = re.search(r"(\d+)\s*kW", value)
            match_hp = re.search(r"(\d+)\s*(LE|hp)", value, re.IGNORECASE)
            if match_kw:
                data["power_kw"] = parse_int(match_kw.group(1))
            if match_hp:
                data["power_hp"] = parse_int(match_hp.group(1))
        else:
            data[mapped] = value

    # Seller info
    for key in ("Kereskedes", "Hirdeto"):
        if key in kv:
            data["seller_name"] = kv[key]
            data["seller_type"] = "dealer"
            break
    if not data.get("seller_name"):
        if "Maganszemely" in kv:
            data["seller_type"] = "private"

    for key in ("Hely", "Telephely", "Cim"):
        if key in kv:
            data["location"] = kv[key]
            break

    return data
