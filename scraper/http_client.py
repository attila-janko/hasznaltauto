from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Optional

import requests


LOGGER = logging.getLogger(__name__)


BLOCKED_PHRASES = [
    "ellenor" ,  # Hungarian: check/verify (partial)
    "nem vagy robot",
    "verification",
    "access denied",
    "too many requests",
]


@dataclass
class FetchResult:
    url: str
    status: int
    text: Optional[str]
    blocked: bool
    skipped: bool
    content_type: str


class HttpClient:
    def __init__(
        self,
        user_agent: str,
        delay_seconds: float = 1.0,
        jitter_seconds: float = 0.5,
        timeout_seconds: float = 20.0,
    ) -> None:
        self.user_agent = user_agent
        self.delay_seconds = delay_seconds
        self.jitter_seconds = jitter_seconds
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.last_request_at = 0.0
        self.robots = None

    def set_robots(self, robots) -> None:
        self.robots = robots

    def _headers(self) -> dict:
        return {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

    def _throttle(self) -> None:
        now = time.time()
        elapsed = now - self.last_request_at
        wait_for = self.delay_seconds - elapsed
        if wait_for > 0:
            time.sleep(wait_for)
        if self.jitter_seconds:
            time.sleep(random.uniform(0, self.jitter_seconds))
        self.last_request_at = time.time()

    def _is_blocked(self, text: str) -> bool:
        text_lower = text.lower()
        for phrase in BLOCKED_PHRASES:
            if phrase in text_lower:
                return True
        return False

    def fetch(
        self,
        url: str,
        ignore_robots: bool = False,
        expect_html: bool = True,
    ) -> FetchResult:
        if self.robots and not ignore_robots:
            if not self.robots.allowed(url):
                LOGGER.warning("Robots blocked: %s", url)
                return FetchResult(url, 0, None, blocked=False, skipped=True, content_type="")

        self._throttle()
        try:
            resp = self.session.get(
                url,
                headers=self._headers(),
                timeout=self.timeout_seconds,
                allow_redirects=True,
            )
        except requests.RequestException as exc:
            LOGGER.warning("Request failed: %s (%s)", url, exc)
            return FetchResult(url, 0, None, blocked=True, skipped=False, content_type="")

        content_type = resp.headers.get("Content-Type", "")
        text = resp.text
        blocked = resp.status_code in (403, 429) or self._is_blocked(text)
        if blocked:
            LOGGER.warning("Blocked response %s for %s", resp.status_code, url)
            return FetchResult(url, resp.status_code, None, blocked=True, skipped=False, content_type=content_type)

        if expect_html and "text/html" not in content_type and "application/xhtml+xml" not in content_type:
            LOGGER.info("Unexpected content type %s for %s", content_type, url)

        return FetchResult(url, resp.status_code, text, blocked=False, skipped=False, content_type=content_type)
