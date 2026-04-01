import random
import re
from typing import Optional, Tuple

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


def build_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def parse_price(price_text: str) -> Tuple[Optional[str], Optional[float]]:
    text = clean_text(price_text)
    if not text:
        return None, None

    currency_match = re.search(r"[^\d\s,\.]", text)
    currency = currency_match.group(0) if currency_match else None

    numeric = re.sub(r"[^\d\.,]", "", text)
    if numeric.count(",") > 1 and "." in numeric:
        numeric = numeric.replace(",", "")
    elif numeric.count(",") >= 1 and "." not in numeric:
        numeric = numeric.replace(",", "")
    else:
        numeric = numeric.replace(",", "")

    try:
        amount = float(numeric) if numeric else None
    except ValueError:
        amount = None

    return currency, amount


def parse_rating(rating_text: str) -> Optional[float]:
    text = clean_text(rating_text)
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    return float(match.group(1)) if match else None


def parse_int(text: str) -> Optional[int]:
    cleaned = re.sub(r"[^\d]", "", clean_text(text))
    return int(cleaned) if cleaned else None


def slugify(text: str) -> str:
    text = clean_text(text).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "query"
