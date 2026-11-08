"""Render read-only Athena acceptance SQL locally; no SDK or cloud access."""
import argparse
from datetime import date, timedelta
from pathlib import Path

SPECS = {
    "HI-Medium": {"last": "2022-09-28", "rows": 31898218, "positive": 35230, "days": 28},
    "LI-Medium": {"last": "2022-09-27", "rows": 31251469, "positive": 16041, "days": 27},
}
ROOT = Path(__file__).resolve().parent
QUERIES = ("01_dataset_partitions.sql", "02_source.sql", "03_sequences.sql",
           "04_lineage.sql", "05_path_inventory.sql")


def render(dataset, query, owner_date=None):
    if dataset not in SPECS or query not in QUERIES:
        raise ValueError("Choose an exact HI/LI dataset and an allowlisted query")
    spec = SPECS[dataset]
    if owner_date is not None:
        if query != "04_lineage.sql":
            raise ValueError("Owner-date chunking is supported only for lineage")
        try:
            owner = date.fromisoformat(owner_date)
        except (ValueError, TypeError):
            raise ValueError("Owner date must be an ISO date inside the selected extract") from None
        if owner.isoformat() != owner_date or not "2022-09-01" <= owner_date <= spec["last"]:
            raise ValueError("Owner date must be inside the selected extract")
        source_last = min(owner + timedelta(days=2),date.fromisoformat(spec["last"])).isoformat()
    else:
        source_last = spec["last"]
    text = (ROOT / query).read_text(encoding="utf-8")
    for name, value in {"DATASET": dataset, "LAST_DATE": spec["last"],
                        "SOURCE_ROWS": spec["rows"], "POSITIVE_ROWS": spec["positive"],
                        "OWNER_DAYS": spec["days"],
                        "OWNER_FILTER": " AND q.start_date='"+owner_date+"'" if owner_date else "",
                        "OWNER_LABEL": owner_date or "ALL",
                        "LINEAGE_FIRST_DATE": owner_date or "2022-09-01",
                        "LINEAGE_LAST_DATE": source_last}.items():
        text = text.replace("__" + name + "__", str(value))
    if "__" in text:
        raise ValueError("Unresolved SQL placeholder")
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, choices=tuple(SPECS))
    parser.add_argument("--query", required=True, choices=QUERIES)
    parser.add_argument("--owner-date", help="Optional lineage-only owner date; all dates must pass")
    args = parser.parse_args()
    print(render(args.dataset, args.query, args.owner_date), end="")


if __name__ == "__main__":
    main()
