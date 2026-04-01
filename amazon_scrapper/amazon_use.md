# amazon_scrapper

A marketplace scraping foundation with an orchestrator design.

It currently supports Amazon and is structured to add Flipkart and other providers next.

## What this project does

- Uses a single orchestrator entrypoint in `main.py`.
- Routes scraping by provider (currently `amazon`).
- Scrapes Amazon search results pages directly using HTTP requests and HTML parsing.
- Exports data as both CSV and JSON.
- Works with public listing pages without API subscriptions.

## Project structure

- main.py: orchestrator caller (non-CLI usage).
- scraper/main.py: `ScraperOrchestrator` (provider routing).
- scraper/providers/amazon.py: Amazon provider implementation.
- scraper/write_output.py: CSV and JSON export.
- scraper/utils.py: parsing and helper utilities.

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
python main.py
```

## Programmatic usage

```python
from scraper import ScraperOrchestrator

orchestrator = ScraperOrchestrator(output_dir="output")
result = orchestrator.scrape(
	provider="amazon",
	query="iphone",
	pages=1,
	max_items=20,
	persist=True,
)

print(result["count"])
```

## Output

Results are saved under:

- output/<query-slug>/csv/<query-slug>.csv
- output/<query-slug>/json/<query-slug>.json

## Notes

- Amazon HTML can change over time, so selectors may need occasional updates.
- The provider extension point is ready; add `FlipkartProvider` and register it in `scraper/main.py`.
- Keep request volume moderate and follow applicable website terms.
