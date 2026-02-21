from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

LOGGER = logging.getLogger(__name__)


@dataclass
class BrowserResult:
    url: str
    text: Optional[str]


class PlaywrightClient:
    def __init__(
        self,
        timeout_seconds: float = 30.0,
        headless: bool = True,
        user_agent: str | None = None,
        locale: str = "hu-HU",
        timezone_id: str = "Europe/Budapest",
        accept_language: str = "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7",
    ) -> None:
        self.timeout_ms = int(timeout_seconds * 1000)
        self.headless = headless
        self.user_agent = user_agent
        self.locale = locale
        self.timezone_id = timezone_id
        self.accept_language = accept_language
        self._playwright = None
        self._browser = None
        self._context = None

    def start(self) -> None:
        if self._playwright:
            return
        from playwright.sync_api import sync_playwright

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.firefox.launch(headless=self.headless)
        context_args = {
            "locale": self.locale,
            "timezone_id": self.timezone_id,
            "extra_http_headers": {"Accept-Language": self.accept_language},
        }
        if self.user_agent:
            context_args["user_agent"] = self.user_agent
        self._context = self._browser.new_context(**context_args)

    def close(self) -> None:
        if self._context:
            self._context.close()
            self._context = None
        if self._browser:
            self._browser.close()
            self._browser = None
        if self._playwright:
            self._playwright.stop()
            self._playwright = None

    def fetch(self, url: str) -> BrowserResult:
        if not self._browser:
            self.start()
        if not self._context:
            self.start()
        page = self._context.new_page()
        try:
            page.goto(url, wait_until="networkidle", timeout=self.timeout_ms)
            html = page.content()
            return BrowserResult(url=url, text=html)
        except Exception as exc:
            LOGGER.warning("Playwright failed: %s (%s)", url, exc)
            return BrowserResult(url=url, text=None)
        finally:
            page.close()
