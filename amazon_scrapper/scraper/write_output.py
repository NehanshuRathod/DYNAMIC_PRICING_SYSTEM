import json
from pathlib import Path
from typing import Dict, List

import pandas as pd

from .utils import slugify


def write_output(query: str, rows: List[Dict], output_dir: str = "output") -> Dict[str, str]:
    query_slug = slugify(query)
    base = Path(output_dir) / query_slug
    json_dir = base / "json"
    csv_dir = base / "csv"

    json_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)

    json_path = json_dir / f"{query_slug}.json"
    csv_path = csv_dir / f"{query_slug}.csv"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    pd.DataFrame(rows).to_csv(csv_path, index=False)

    return {
        "json": str(json_path),
        "csv": str(csv_path),
    }
