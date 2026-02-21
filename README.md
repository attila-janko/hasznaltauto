# Hasznaltauto.hu Scraper

This project provides a polite, robots-aware scraper for hasznaltauto.hu. It collects listing detail pages into a SQLite database and stores structured fields plus raw attribute maps.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional Playwright fallback (for JS/anti-bot pages):

```bash
pip install -r requirements-playwright.txt
python -m playwright install
```

If you are getting 403s with requests, try browser-only mode:

```bash
python scrape.py --playwright --browser-only --headful
```

If you hit a Cloudflare challenge, run a one-time manual auth and save cookies:

```bash
python scrape.py --playwright --browser-only --headful --manual-auth --save-storage-state data/storage_state.json --no-sitemap --max-pages 1
```

Then reuse the session:

```bash
python scrape.py --playwright --browser-only --storage-state data/storage_state.json --no-sitemap --max-pages 1 --max-listings 50
```

## Usage

Basic scrape (defaults to `szemelyauto`, respects robots.txt):

```bash
python scrape.py --max-listings 200
```

Multiple categories:

```bash
python scrape.py --category szemelyauto --category teherauto --max-listings 300
```

Disable sitemap and rely on category pages only:

```bash
python scrape.py --no-sitemap --max-pages 1
```

Force sitemap fetches through Playwright:

```bash
python scrape.py --playwright --sitemap-via-browser
```

Store raw HTML for later re-parsing:

```bash
python scrape.py --store-html
```

## Output

SQLite database at `data/hasznaltauto.sqlite` with the `listings` table. Fields include:

- `ad_id` (primary key)
- `title`, `price_huf`, `year`, `mileage_km`, `fuel`, `engine_cc`, `power_kw`, `power_hp`, ...
- `equipment_json`, `images_json`, `attributes_json`
- `raw_html` (optional)

## Notes

- The scraper enforces `robots.txt` and skips disallowed URLs.
- Rate-limiting and jitter are enabled by default; adjust with `--delay` and `--jitter`.
- Image URLs are collected but not downloaded.
