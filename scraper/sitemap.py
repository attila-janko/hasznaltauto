from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import Iterable, List, Optional
from urllib.parse import urlparse

from .http_client import FetchResult
from .parse import CATEGORY_PATTERNS


LOGGER = logging.getLogger(__name__)


def _parse_xml(xml_text: str) -> ET.Element:
    return ET.fromstring(xml_text)


def _looks_like_sitemap(xml_text: str) -> bool:
    head = xml_text.lstrip()[:300].lower()
    return "<urlset" in head or "<sitemapindex" in head


def _namespace(tag: str) -> str:
    if tag.startswith("{"):
        return tag.split("}")[0] + "}"
    return ""


def parse_sitemap(xml_text: str) -> tuple[list[str], list[str]]:
    root = _parse_xml(xml_text)
    ns = _namespace(root.tag)

    if root.tag.endswith("sitemapindex"):
        locs = [loc.text.strip() for loc in root.findall(f".//{ns}loc") if loc.text]
        return [], locs

    locs = [loc.text.strip() for loc in root.findall(f".//{ns}loc") if loc.text]
    return locs, []


def _is_listing_url(url: str, categories: Optional[List[str]] = None) -> bool:
    parsed = urlparse(url)
    if not parsed.path:
        return False
    allowed = categories or CATEGORY_PATTERNS
    for category in allowed:
        if f"/{category}/" in parsed.path:
            return True
    return False


def _write_debug(debug_dir: Optional[str], filename: str, content: str) -> None:
    if not debug_dir:
        return
    try:
        import os

        os.makedirs(debug_dir, exist_ok=True)
        path = os.path.join(debug_dir, filename)
        with open(path, "w", encoding="utf-8", errors="ignore") as handle:
            handle.write(content)
    except Exception:
        LOGGER.warning("Failed to write debug file: %s", filename)


def fetch_sitemap_urls(
    client,
    base_url: str,
    categories: Optional[List[str]] = None,
    max_urls: Optional[int] = None,
    browser=None,
    prefer_browser: bool = False,
    debug_dir: Optional[str] = None,
) -> List[str]:
    sitemap_index = base_url.rstrip("/") + "/sitemap/sitemap_index.xml"
    result = client.fetch(sitemap_index, ignore_robots=True, expect_html=False)
    if result.text is None and browser and prefer_browser:
        result_text = browser.fetch(sitemap_index).text
        if result_text:
            result = FetchResult(sitemap_index, 200, result_text, False, False, "text/xml")
    if result.text is None:
        LOGGER.warning("Sitemap index not accessible: %s", sitemap_index)
        return []
    if not _looks_like_sitemap(result.text):
        LOGGER.warning("Sitemap index did not return XML: %s", sitemap_index)
        _write_debug(debug_dir, "sitemap_index.html", result.text)
        return []
    try:
        page_urls, index_urls = parse_sitemap(result.text)
    except ET.ParseError:
        LOGGER.warning("Sitemap index XML parse failed: %s", sitemap_index)
        _write_debug(debug_dir, "sitemap_index_parse_error.html", result.text)
        return []

    urls: List[str] = []
    pending_indexes = index_urls or []
    visited = set()

    while pending_indexes:
        sitemap_url = pending_indexes.pop(0)
        if sitemap_url in visited:
            continue
        visited.add(sitemap_url)

        if sitemap_url != sitemap_index:
            result = client.fetch(sitemap_url, ignore_robots=True, expect_html=False)
            if result.text is None and browser and prefer_browser:
                result_text = browser.fetch(sitemap_url).text
                if result_text:
                    result = FetchResult(sitemap_url, 200, result_text, False, False, "text/xml")
            if result.text is None:
                LOGGER.warning("Sitemap not accessible: %s", sitemap_url)
                continue

        if not _looks_like_sitemap(result.text):
            LOGGER.warning("Sitemap did not return XML: %s", sitemap_url)
            _write_debug(debug_dir, "sitemap_non_xml.html", result.text)
            continue
        try:
            page_urls, index_urls = parse_sitemap(result.text)
        except ET.ParseError:
            LOGGER.warning("Sitemap XML parse failed: %s", sitemap_url)
            _write_debug(debug_dir, "sitemap_parse_error.html", result.text)
            continue
        if index_urls:
            pending_indexes.extend(index_urls)
        if page_urls:
            for url in page_urls:
                if _is_listing_url(url, categories):
                    urls.append(url)
                    if max_urls and len(urls) >= max_urls:
                        return urls

    return urls
