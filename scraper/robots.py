from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

from .http_client import FetchResult


LOGGER = logging.getLogger(__name__)


class RobotsPolicy:
    def __init__(self, base_url: str, user_agent: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent
        self._parser: Optional[RobotFileParser] = None
        self._loaded = False

    def load(self, fetcher) -> None:
        robots_url = urljoin(self.base_url + "/", "robots.txt")
        result: FetchResult = fetcher(robots_url, ignore_robots=True, expect_html=False)
        if result.text is None:
            LOGGER.warning("Could not load robots.txt (%s)", robots_url)
            self._loaded = True
            self._parser = None
            return

        parser = RobotFileParser()
        parser.parse(result.text.splitlines())
        self._parser = parser
        self._loaded = True
        LOGGER.info("Loaded robots.txt (%s)", robots_url)

    def allowed(self, url: str) -> bool:
        if not self._loaded or self._parser is None:
            return True
        return self._parser.can_fetch(self.user_agent, url)
