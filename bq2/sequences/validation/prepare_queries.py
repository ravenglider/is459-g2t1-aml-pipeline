"""Write individual acceptance statements locally; never submit AWS queries."""
import argparse
import hashlib
import json
from pathlib import Path

from render_acceptance import ROOT, SPECS, render
from review_run import dates


def split_sql(text):
    """Split statements without treating quoted/comment semicolons as separators."""
    output, buffer, state, has_code, i = [], [], "normal", False, 0
    while i < len(text):
        char, following = text[i], text[i:i+2]
        if state == "line":
            buffer.append(char)
            if char == "\n": state = "normal"
        elif state == "block":
            if following == "*/":
                buffer.extend(following); i += 1; state = "normal"
            else: buffer.append(char)
        elif state in ("single", "double"):
            quote = "'" if state == "single" else '"'
            buffer.append(char)
            if char == quote:
                if i+1 < len(text) and text[i+1] == quote:
                    buffer.append(quote); i += 1
                else: state = "normal"
        elif following in ("--", "/*"):
            buffer.extend(following); i += 1
            state = "line" if following == "--" else "block"
        elif char in ("'", '"'):
            has_code = True; state = "single" if char == "'" else "double"
            buffer.append(char)
        elif char == ";":
            if has_code: output.append("".join(buffer).strip()+";\n")
            buffer, has_code = [], False
        else:
            buffer.append(char)
            if not char.isspace(): has_code = True
        i += 1
    if state in ("single", "double", "block"):
        raise ValueError("Unterminated SQL quote/comment")
    if has_code: output.append("".join(buffer).strip()+";\n")
    return output


def query_plan():
    """Return all statements, phases and hashes, including both lineage options."""
    plan = []

    def add(path, text, phase, dataset=None, owner_date=None):
        statements = split_sql(text)
        if len(statements) != 1:
            raise ValueError("Exactly one statement required per query file")
        statement = statements[0]
        plan.append(dict(path=path, sql=statement, phase=phase, dataset=dataset,
                         owner_date=owner_date,
                         sha256=hashlib.sha256(statement.encode("utf-8")).hexdigest()))

    add("00-schema.sql", (ROOT/"00_schema.sql").read_text(encoding="utf-8"), "published")
    add("99-both-datasets-partitions.sql", (ROOT/"01_partitions.sql").read_text(encoding="utf-8"), "both_published")
    for dataset in SPECS:
        add(dataset+"/01-partitions.sql",render(dataset,"01_dataset_partitions.sql"),"published",dataset)
        source = split_sql(render(dataset,"02_source.sql"))
        sequences = split_sql(render(dataset,"03_sequences.sql"))
        if len(source) != 2 or len(sequences) != 4:
            raise ValueError("Acceptance template statement count changed; review the plan")
        for name, statement in zip(("02-source-summary.sql","03-source-daily.sql"),source):
            add(dataset+"/"+name,statement,"source",dataset)
        for name, statement in zip(("04-sequence-counts.sql","05-sequence-values.sql",
                                    "06-sequence-duplicates.sql","07-sequence-labels.sql"),sequences):
            add(dataset+"/"+name,statement,"published",dataset)
        add(dataset+"/08-lineage-all.sql",render(dataset,"04_lineage.sql"),"lineage_all",dataset)
        add(dataset+"/09-paths.sql",render(dataset,"05_path_inventory.sql"),"published",dataset)
        for day in dates(dataset):
            add(dataset+"/lineage/"+day+".sql",render(dataset,"04_lineage.sql",day),"lineage_date",dataset,day)
    return plan


def write_plan(output):
    plan = query_plan()
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    for item in plan:
        path = output/item["path"]
        path.parent.mkdir(parents=True,exist_ok=True)
        with path.open("x",encoding="utf-8",newline="\n") as handle:
            handle.write(item["sql"])
    summary = [dict((k,v) for k,v in item.items() if k != "sql") for item in plan]
    with (output/"query-plan.json").open("x",encoding="utf-8") as handle:
        json.dump(dict(status="PREPARED_NOT_EXECUTED",queries=summary),handle,indent=2)
    return len(plan)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",required=True,type=Path,help="New local directory; existing paths rejected")
    args = parser.parse_args()
    print(json.dumps(dict(status="PREPARED_NOT_EXECUTED",queries=write_plan(args.output),output=str(args.output))))


if __name__ == "__main__":
    main()
