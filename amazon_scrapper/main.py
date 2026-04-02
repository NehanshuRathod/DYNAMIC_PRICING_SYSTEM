from typing import Dict, Optional

from scraper import ScraperOrchestrator


def scrape_marketplace(
    provider: str,
    query: str,
    pages: int = 1,
    max_items: Optional[int] = 50,
    output_dir: str = "output",
) -> Dict:
    orchestrator = ScraperOrchestrator(output_dir=output_dir)
    return orchestrator.scrape(
        provider=provider,
        query=query,
        pages=pages,
        max_items=max_items,
        persist=True,
    )


def main() -> None:
    # Orchestrator entrypoint; replace values with your service inputs.
    result = scrape_marketplace(
        provider="amazon",
        query="iphone",
        pages=1,
        max_items=10,
    )
    print(f"Provider: {result['provider']}")
    print(f"Query: {result['query']}")
    print(f"Rows: {result['count']}")
    if "files" in result:
        print(f"CSV:  {result['files']['csv']}")
        print(f"JSON: {result['files']['json']}")


if __name__ == "__main__":
    main()
