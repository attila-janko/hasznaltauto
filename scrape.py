from __future__ import annotations

import argparse
import logging
import os
import re
from pathlib import Path
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup
from tqdm import tqdm

from scraper.browser import PlaywrightClient
from scraper.db import connect_db, init_db, listing_exists, upsert_listing
from scraper.http_client import HttpClient
from scraper.parse import CATEGORY_PATTERNS, extract_listing_urls, parse_detail
from scraper.robots import RobotsPolicy
from scraper.sitemap import fetch_sitemap_urls


LOGGER = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hasznaltauto.hu scraper (polite and robots-aware).")
    parser.add_argument("--base-url", default="https://www.hasznaltauto.hu", help="Base site URL")
    parser.add_argument("--category", action="append", help="Category path (repeatable)")
    parser.add_argument("--max-pages", type=int, default=1, help="Max listing pages to fetch per category")
    parser.add_argument("--max-listings", type=int, default=500, help="Max detail listings to scrape")
    parser.add_argument("--delay", type=float, default=1.0, help="Minimum delay between requests")
    parser.add_argument("--jitter", type=float, default=0.5, help="Random jitter added to delay")
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout seconds")
    parser.add_argument("--out", default="data/hasznaltauto.sqlite", help="SQLite output path")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="User agent")
    parser.add_argument("--playwright", action="store_true", help="Enable Playwright fallback")
    parser.add_argument("--browser-only", action="store_true", help="Use Playwright for all fetches")
    parser.add_argument("--sitemap-via-browser", action="store_true", help="Use Playwright for sitemap fetches")
    parser.add_argument("--headful", action="store_true", help="Run Playwright with visible browser")
    parser.add_argument("--store-html", action="store_true", help="Store raw HTML in SQLite")
    parser.add_argument("--no-sitemap", dest="use_sitemap", action="store_false", help="Disable sitemap crawl")
    parser.set_defaults(use_sitemap=True)
    parser.add_argument("--no-resume", dest="resume", action="store_false", help="Re-scrape existing ads")
    parser.set_defaults(resume=True)
    return parser.parse_args()


def extract_pagination_urls(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    urls = []
    for link in soup.find_all("a", href=True):
        href = link.get("href", "").strip()
        if not href:
            continue
        if "page" not in href and "oldal" not in href and "lap" not in href:
            continue
        if href.startswith("//"):
            href = "https:" + href
        full = href if href.startswith("http") else base_url.rstrip("/") + "/" + href.lstrip("/")
        if full not in urls:
            urls.append(full)
    return urls


def fetch_html(
    url: str,
    client: HttpClient,
    robots: RobotsPolicy,
    browser: Optional[PlaywrightClient],
    use_playwright: bool,
    browser_only: bool,
    expect_html: bool = True,
) -> Optional[str]:
    if not robots.allowed(url):
        LOGGER.info("Robots blocked: %s", url)
        return None

    if browser_only:
        if not browser:
            return None
        return browser.fetch(url).text

    result = client.fetch(url, expect_html=expect_html)
    if result.text is None and result.skipped:
        return None
    if result.text is None and use_playwright and browser and not result.skipped:
        return browser.fetch(url).text
    return result.text


def get_listing_page_urls(
    client: HttpClient,
    robots: RobotsPolicy,
    base_url: str,
    categories: List[str],
    max_pages: int,
    use_playwright: bool,
    browser: Optional[PlaywrightClient],
    browser_only: bool,
) -> List[Tuple[str, int]]:
    listing_refs: List[Tuple[str, int]] = []
    for category in categories:
        start_url = base_url.rstrip("/") + "/" + category.strip("/")
        queue = [start_url]
        visited = set()
        while queue and len(visited) < max_pages:
            page_url = queue.pop(0)
            if page_url in visited:
                continue
            visited.add(page_url)

            html = fetch_html(
                page_url,
                client=client,
                robots=robots,
                browser=browser,
                use_playwright=use_playwright,
                browser_only=browser_only,
            )
            if html is None:
                LOGGER.warning("Failed to fetch listing page: %s", page_url)
                continue

            listing_refs.extend(extract_listing_urls(html, base_url, categories))
            for next_url in extract_pagination_urls(html, base_url):
                if next_url in visited:
                    continue
                if not robots.allowed(next_url):
                    LOGGER.info("Pagination blocked by robots: %s", next_url)
                    continue
                queue.append(next_url)

    return listing_refs


def parse_ad_id(url: str) -> Optional[int]:
    match = re.search(r"-(\d+)$", url)
    if match:
        return int(match.group(1))
    return None


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    categories = args.category or ["szemelyauto"]
    if args.browser_only and not args.playwright:
        args.playwright = True

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    client = HttpClient(
        user_agent=args.user_agent,
        delay_seconds=args.delay,
        jitter_seconds=args.jitter,
        timeout_seconds=args.timeout,
    )
    robots = RobotsPolicy(args.base_url, args.user_agent)
    client.set_robots(robots)
    robots.load(client.fetch)

    browser = (
        PlaywrightClient(
            timeout_seconds=args.timeout,
            headless=not args.headful,
            user_agent=args.user_agent,
        )
        if args.playwright
        else None
    )

    try:
        listing_urls: List[str] = []
        if args.use_sitemap:
            LOGGER.info("Loading sitemap URLs...")
            listing_urls = fetch_sitemap_urls(
                client,
                base_url=args.base_url,
                categories=categories,
                max_urls=args.max_listings,
                browser=browser,
                prefer_browser=args.sitemap_via_browser or args.browser_only,
            )
            LOGGER.info("Sitemap URLs collected: %s", len(listing_urls))

        if not listing_urls:
            LOGGER.info("Falling back to category listing pages...")
            listing_refs = get_listing_page_urls(
                client,
                robots,
                args.base_url,
                categories,
                max_pages=args.max_pages,
                use_playwright=args.playwright,
                browser=browser,
                browser_only=args.browser_only,
            )
            listing_urls = [url for url, _ in listing_refs]

        if args.max_listings and len(listing_urls) > args.max_listings:
            listing_urls = listing_urls[: args.max_listings]

        conn = connect_db(str(out_path))
        init_db(conn)

        for url in tqdm(listing_urls, desc="Listings"):
            ad_id = parse_ad_id(url)
            if args.resume and ad_id and listing_exists(conn, ad_id):
                continue

            html = fetch_html(
                url,
                client=client,
                robots=robots,
                browser=browser,
                use_playwright=args.playwright,
                browser_only=args.browser_only,
            )
            if html is None:
                LOGGER.warning("Failed to fetch detail page: %s", url)
                continue

            data = parse_detail(html, url, args.base_url)
            if args.store_html:
                data["raw_html"] = html

            if not data.get("ad_id") and ad_id:
                data["ad_id"] = ad_id
            if not data.get("ad_id"):
                LOGGER.warning("Missing ad_id for %s", url)
                continue

            upsert_listing(conn, data)
    finally:
        if browser:
            browser.close()


if __name__ == "__main__":
    main()
