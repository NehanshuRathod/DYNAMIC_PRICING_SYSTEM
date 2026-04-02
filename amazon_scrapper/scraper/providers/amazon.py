from dataclasses import asdict
from time import sleep
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

from ..utils import build_headers, clean_text, parse_int, parse_price, parse_rating

COUNTRY_DOMAIN_MAP = {
    "US": "www.amazon.com",
    "IN": "www.amazon.in",
    "UK": "www.amazon.co.uk",
    "DE": "www.amazon.de",
    "FR": "www.amazon.fr",
    "IT": "www.amazon.it",
    "ES": "www.amazon.es",
    "CA": "www.amazon.ca",
}


class AmazonProvider:
    provider_name = "amazon"

    def __init__(self, country_code: str = "IN", timeout: int = 25, delay_seconds: float = 1.2) -> None:
        self.country_code = country_code.upper()
        self.domain = COUNTRY_DOMAIN_MAP.get(self.country_code, COUNTRY_DOMAIN_MAP["IN"])
        self.timeout = timeout
        self.delay_seconds = delay_seconds
        self.session = requests.Session()

    def _fetch(self, url: str) -> str:
        response = self.session.get(url, headers=build_headers(), timeout=self.timeout)
        response.raise_for_status()
        return response.text

    def _search_url(self, query: str, page: int) -> str:
        q = quote_plus(query)
        return f"https://{self.domain}/s?k={q}&page={page}"

    def _parse_search_page(self, html: str) -> Tuple[List[Dict], Optional[str]]:
        soup = BeautifulSoup(html, "lxml")
        rows: List[Dict] = []

        for card in soup.select('div[data-component-type="s-search-result"][data-asin]'):
            asin = clean_text(card.get("data-asin", ""))
            if not asin:
                continue

            title_el = (
                card.select_one("h2 a span")
                or card.select_one("a.a-link-normal .a-size-medium")
                or card.select_one("a h2")
            )
            link_el = (
                card.select_one('a[href*="/dp/"]')
                or card.select_one('a[href*="/gp/"]')
                or card.select_one("h2 a")
                or card.select_one("a.a-link-normal")
            )
            image_el = card.select_one("img.s-image")
            price_el = card.select_one("span.a-price span.a-offscreen")
            rating_el = card.select_one("span.a-icon-alt")
            reviews_el = card.select_one("span.a-size-base.s-underline-text") or card.select_one(
                "span[aria-label$='ratings'], span[aria-label$='rating']"
            )

            title = clean_text(title_el.get_text()) if title_el else ""
            href = link_el.get("href") if link_el else ""
            link = urljoin(f"https://{self.domain}", href)
            image_url = clean_text(image_el.get("src", "")) if image_el else ""

            price_text = clean_text(price_el.get_text()) if price_el else ""
            currency, price = parse_price(price_text)

            rating_text = clean_text(rating_el.get_text()) if rating_el else ""
            rating = parse_rating(rating_text)

            reviews_text = clean_text(reviews_el.get_text()) if reviews_el else ""
            reviews = parse_int(reviews_text)

            card_text = clean_text(card.get_text(" "))
            is_prime = "prime" in card_text.lower()
            is_sponsored = "sponsored" in card_text.lower()

            if title and link:
                rows.append(
                    {
                        "provider": self.provider_name,
                        "title": title,
                        "link": link,
                        "asin": asin,
                        "price": price,
                        "currency": currency,
                        "rating": rating,
                        "reviews": reviews,
                        "image_url": image_url,
                        "is_prime": is_prime,
                        "is_sponsored": is_sponsored,
                    }
                )

        next_button = soup.select_one("a.s-pagination-next")
        next_url = None
        if next_button and "s-pagination-disabled" not in (next_button.get("class") or []):
            href = next_button.get("href")
            if href:
                next_url = urljoin(f"https://{self.domain}", href)

        return rows, next_url

    def search(self, query: str, pages: int = 1, max_items: Optional[int] = None) -> List[Dict]:
        all_items: List[Dict] = []
        next_url: Optional[str] = self._search_url(query, page=1)
        page_count = 0

        while next_url and page_count < pages:
            html = self._fetch(next_url)
            rows, parsed_next_url = self._parse_search_page(html)
            all_items.extend(rows)

            if max_items is not None and len(all_items) >= max_items:
                all_items = all_items[:max_items]
                break

            next_url = parsed_next_url
            page_count += 1
            if next_url and self.delay_seconds > 0:
                sleep(self.delay_seconds)

        return all_items
