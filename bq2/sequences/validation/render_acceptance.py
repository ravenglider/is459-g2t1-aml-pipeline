"""Render read-only Athena acceptance SQL locally; no SDK or cloud access."""
import argparse
from pathlib import Path

SPECS = {
    "HI-Medium": {"last": "2022-09-28", "rows": 31898218, "positive": 35230, "days": 28},
    "LI-Medium": {"last": "2022-09-27", "rows": 31251469, "positive": 16041, "days": 27},
}
ROOT = Path(__file__).resolve().parent
QUERIES = ("02_source.sql", "03_sequences.sql", "04_lineage.sql", "05_path_inventory.sql")


def render(dataset, query):
    if dataset not in SPECS or query not in QUERIES:
        raise ValueError("Choose an exact HI/LI dataset and an allowlisted query")
    spec = SPECS[dataset]
    text = (ROOT / query).read_text(encoding="utf-8")
    for name, value in {"DATASET": dataset, "LAST_DATE": spec["last"],
                        "SOURCE_ROWS": spec["rows"], "POSITIVE_ROWS": spec["positive"],
                        "OWNER_DAYS": spec["days"]}.items():
        text = text.replace("__" + name + "__", str(value))
    if "__" in text:
        raise ValueError("Unresolved SQL placeholder")
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, choices=tuple(SPECS))
    parser.add_argument("--query", required=True, choices=QUERIES)
    args = parser.parse_args()
    print(render(args.dataset, args.query), end="")


if __name__ == "__main__":
    main()
