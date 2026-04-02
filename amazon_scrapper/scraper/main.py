from typing import Dict, List, Optional

from .providers import AmazonProvider
from .write_output import write_output


class ScraperOrchestrator:
    """Orchestrates data collection from marketplace providers."""

    def __init__(self, output_dir: str = "output") -> None:
        self.output_dir = output_dir
        self.providers = {
            "amazon": AmazonProvider(),
            # "flipkart": FlipkartProvider(),
        }

    def scrape(
        self,
        provider: str,
        query: str,
        pages: int = 1,
        max_items: Optional[int] = 50,
        persist: bool = True,
    ) -> Dict:
        provider_key = provider.strip().lower()
        if provider_key not in self.providers:
            supported = ", ".join(sorted(self.providers.keys()))
            raise ValueError(f"Unsupported provider '{provider}'. Supported providers: {supported}")

        rows = self.providers[provider_key].search(query=query, pages=pages, max_items=max_items)

        result = {
            "provider": provider_key,
            "query": query,
            "count": len(rows),
            "data": rows,
        }

        if persist:
            result["files"] = write_output(query=query, rows=rows, output_dir=self.output_dir)

        return result

    def scrape_all(self, query: str, pages: int = 1, max_items: Optional[int] = 50, persist: bool = True) -> Dict:
        combined: List[Dict] = []
        provider_counts: Dict[str, int] = {}

        for provider_name in self.providers:
            rows = self.providers[provider_name].search(query=query, pages=pages, max_items=max_items)
            provider_counts[provider_name] = len(rows)
            combined.extend(rows)

        result = {
            "query": query,
            "total_count": len(combined),
            "provider_counts": provider_counts,
            "data": combined,
        }

        if persist:
            result["files"] = write_output(query=query, rows=combined, output_dir=self.output_dir)

        return result


def run_scrape(provider: str, query: str, pages: int = 1, max_items: Optional[int] = 50) -> Dict:
    orchestrator = ScraperOrchestrator()
    return orchestrator.scrape(provider=provider, query=query, pages=pages, max_items=max_items, persist=True)
