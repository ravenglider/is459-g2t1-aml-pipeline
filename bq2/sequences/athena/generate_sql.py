"""Generate the Athena SQL that builds bq2_sequences (Role 5, v1 schema).

One template produces every statement, so pairs/triples and HI/LI can never drift
apart. Run:  python3 generate_sql.py   -> writes the .sql files in this folder.

Chain rule (sequence_rules.md, contract v1), for each adjacent pair (a, b):
    a.dataset = b.dataset
    a.to_bank = b.from_bank AND a.to_account = b.from_account
    a.txn_ts < b.txn_ts <= a.txn_ts + 24 hours   (strict start, inclusive 24h)
No whole-chain limit, all branches kept, no repeated transaction in a chain.

Performance: a plain self-join on (bank, account) compares every transaction of a
busy account with every other one across all 28 days before the time filter runs.
Because a valid next hop is always on the same day or the next day, each row of
the earlier transaction is expanded to two "key days" (its own day, day + 1) and
joined on (bank, account, day) with equality. That keeps Athena's hash join small
and finds every valid link exactly once (the later transaction has one day only).
"""
from datetime import date, timedelta
from pathlib import Path

DB = "is459_g2t1_aml"
SRC = f"{DB}.transactions_clean"
BUCKET = "s3://is459-g2t1-aml-processed-bucket"
EPOCH = "DATE '2022-08-01'"  # any fixed date before the data; only differences matter
HERE = Path(__file__).resolve().parent


def src_cte(name, ds, d_from=None, d_to=None):
    where = f"dataset = '{ds}'"
    if d_from:
        where += f" AND txn_date BETWEEN '{d_from}' AND '{d_to}'"
    return f"""{name} AS (
  SELECT txn_id, txn_ts, from_bank, from_account, to_bank, to_account,
         payment_format, payment_currency, receiving_currency, is_laundering,
         date_diff('day', {EPOCH}, CAST(txn_date AS date)) AS day_idx
  FROM {SRC}
  WHERE {where}
)"""


def link_join(prev, nxt):
    """Join conditions linking hop `prev` (columns p_*) to transaction table alias `nxt`."""
    return (f"{nxt}.from_bank = {prev}_to_bank AND {nxt}.from_account = {prev}_to_account "
            f"AND {nxt}.day_idx = k.key_day "
            f"AND {nxt}.txn_ts > {prev}_ts AND {nxt}.txn_ts <= {prev}_ts + INTERVAL '1' DAY")


HOP_COLS = ["id", "ts", "m", "pc", "rc", "l", "to_bank", "to_account", "day"]


def hop_select(alias, p):
    return (f"{alias}.txn_id AS {p}_id, {alias}.txn_ts AS {p}_ts, {alias}.payment_format AS {p}_m, "
            f"{alias}.payment_currency AS {p}_pc, {alias}.receiving_currency AS {p}_rc, "
            f"{alias}.is_laundering AS {p}_l, {alias}.to_bank AS {p}_to_bank, "
            f"{alias}.to_account AS {p}_to_account, {alias}.day_idx AS {p}_day")


def chain_ctes(ds, length, window=None):
    """CTEs ending in `chains` with hop columns a_*, b_* (and c_* for triples)."""
    if window:
        d_from, d_to = window
        ta = src_cte("ta", ds, d_from, d_to)
        tb = src_cte("tb", ds, d_from, shift(d_to, 1))
        tc = src_cte("tc", ds, d_from, shift(d_to, 2))
    else:
        ta, tb, tc = src_cte("ta", ds), src_cte("tb", ds), src_cte("tc", ds)
    ctes = [ta, tb]
    ctes.append(f"""hop_a AS (
  SELECT {hop_select('ta', 'a')} FROM ta
)""")
    ctes.append(f"""pairs AS (
  SELECT h.*, {hop_select('tb', 'b')}
  FROM hop_a h
  CROSS JOIN UNNEST(ARRAY[h.a_day, h.a_day + 1]) AS k(key_day)
  JOIN tb ON {link_join('h.a', 'tb')}
  WHERE tb.txn_id <> h.a_id
)""")
    if length == 2:
        ctes.append("chains AS (SELECT * FROM pairs)")
    else:
        ctes.append(tc)
        ctes.append(f"""chains AS (
  SELECT p.*, {hop_select('tc', 'c')}
  FROM pairs p
  CROSS JOIN UNNEST(ARRAY[p.b_day, p.b_day + 1]) AS k(key_day)
  JOIN tc ON {link_join('p.b', 'tc')}
  WHERE tc.txn_id <> p.a_id AND tc.txn_id <> p.b_id
)""")
    return ctes


def output_select(ds, length):
    hops = ["a", "b", "c"][:length]
    arr = lambda col: "ARRAY[" + ", ".join(f"{h}_{col}" for h in hops) + "]"
    seq = lambda col: " || ' -> ' || ".join(f"{h}_{col}" for h in hops)
    ids_csv = " || ',' || ".join(f"CAST({h}_id AS varchar)" for h in hops)
    lab_sum = " + ".join(f"{h}_l" for h in hops)
    gaps = ", ".join(f"date_diff('second', {hops[i]}_ts, {hops[i+1]}_ts)" for i in range(length - 1))
    last = hops[-1]
    return f"""SELECT
  lower(to_hex(sha256(to_utf8('v1' || chr(10) || '{ds}' || chr(10) || {ids_csv})))) AS sequence_id,
  'v1' AS rule_version,
  CAST({length} AS integer) AS sequence_length,
  {arr('id')} AS txn_ids,
  {arr('ts')} AS txn_timestamps,
  {arr('m')} AS payment_methods,
  {seq('m')} AS method_sequence,
  {arr('pc')} AS payment_currencies,
  {arr('rc')} AS receiving_currencies,
  {seq('pc')} AS payment_currency_sequence,
  {seq('rc')} AS receiving_currency_sequence,
  {arr('l')} AS txn_laundering_labels,
  CAST({lab_sum} AS integer) AS laundering_count,
  CASE WHEN {lab_sum} > 0 THEN 1 ELSE 0 END AS any_laundering,
  CASE WHEN {lab_sum} = {length} THEN 1 ELSE 0 END AS all_laundering,
  {last}_id AS final_txn_id,
  {last}_l AS final_is_laundering,
  ARRAY[{gaps}] AS gap_seconds,
  date_diff('second', a_ts, {last}_ts) AS duration_seconds,
  '{ds}' AS dataset,
  date_format(a_ts, '%Y-%m-%d') AS start_date
FROM chains"""


def body(ds, length, window=None):
    return "WITH\n" + ",\n".join(chain_ctes(ds, length, window)) + "\n" + output_select(ds, length)


def ctas(table, prefix, ds, length, window=None):
    return f"""CREATE TABLE {DB}.{table}
WITH (
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = '{BUCKET}/{prefix}/',
  partitioned_by = ARRAY['dataset', 'start_date']
) AS
{body(ds, length, window)}"""


def insert(table, ds, length, window=None):
    return f"INSERT INTO {DB}.{table}\n{body(ds, length, window)}"


def shift(d, n):
    return (date.fromisoformat(d) + timedelta(days=n)).isoformat()


def size_query(ds):
    ctes = chain_ctes(ds, 2)
    return ("-- Exact pair and triple counts for " + ds + ", without writing anything.\n"
            
            "WITH\n" + ",\n".join(ctes) + ",\n"
            "outdeg AS (SELECT a_id AS id, COUNT(*) AS n FROM pairs GROUP BY a_id)\n"
            "SELECT\n"
            "  (SELECT COUNT(*) FROM pairs) AS pairs,\n"
            "  (SELECT SUM(o.n) FROM pairs p JOIN outdeg o ON o.id = p.b_id) AS triples")


def day_count_query(ds, d):
    ctes = chain_ctes(ds, 3, (d, d))
    return ("WITH\n" + ",\n".join(ctes) + "\n"
            "SELECT (SELECT COUNT(*) FROM pairs) AS pairs, (SELECT COUNT(*) FROM chains) AS triples")


def dates(d_from, d_to):
    out, d = [], d_from
    while d <= d_to:
        out.append(d)
        d = shift(d, 1)
    return out


STRUCTURE_CHECKS = [
    ("bad_rule_version", "rule_version <> 'v1'"),
    ("bad_length", "sequence_length NOT IN (2, 3)"),
    ("bad_array_lengths", "cardinality(txn_ids) <> sequence_length OR cardinality(txn_timestamps) <> sequence_length"
     " OR cardinality(payment_methods) <> sequence_length OR cardinality(payment_currencies) <> sequence_length"
     " OR cardinality(receiving_currencies) <> sequence_length OR cardinality(txn_laundering_labels) <> sequence_length"
     " OR cardinality(gap_seconds) <> sequence_length - 1"),
    ("repeated_ids", "cardinality(array_distinct(txn_ids)) <> sequence_length"),
    ("bad_gap_values", "gap_seconds <> transform(sequence(1, sequence_length - 1),"
     " i -> date_diff('second', element_at(txn_timestamps, i), element_at(txn_timestamps, i + 1)))"),
    ("gap_out_of_range", "cardinality(filter(gap_seconds, g -> g <= 0 OR g > 86400)) > 0"),
    ("bad_duration", "duration_seconds <> date_diff('second', element_at(txn_timestamps, 1), element_at(txn_timestamps, -1))"),
    ("bad_final_id", "final_txn_id <> element_at(txn_ids, -1)"),
    ("bad_final_label", "final_is_laundering <> element_at(txn_laundering_labels, -1)"),
    ("bad_label_values", "cardinality(filter(txn_laundering_labels, x -> x NOT IN (0, 1))) > 0"),
    ("bad_label_count", "laundering_count <> cardinality(filter(txn_laundering_labels, x -> x = 1))"),
    ("bad_any_laundering", "any_laundering <> (CASE WHEN laundering_count > 0 THEN 1 ELSE 0 END)"),
    ("bad_all_laundering", "all_laundering <> (CASE WHEN laundering_count = sequence_length THEN 1 ELSE 0 END)"),
    ("bad_method_string", "method_sequence <> array_join(payment_methods, ' -> ')"),
    ("bad_currency_strings", "payment_currency_sequence <> array_join(payment_currencies, ' -> ')"
     " OR receiving_currency_sequence <> array_join(receiving_currencies, ' -> ')"),
    ("bad_start_date", "start_date <> date_format(element_at(txn_timestamps, 1), '%Y-%m-%d')"),
    ("bad_sequence_id", "sequence_id <> lower(to_hex(sha256(to_utf8('v1' || chr(10) || dataset || chr(10)"
     " || array_join(transform(txn_ids, x -> CAST(x AS varchar)), ',')))))"),
]


def structure_query(table, where=None):
    """Every derived column recomputed from the row's own arrays. Every count must be 0."""
    cols = [f"  COUNT_IF(coalesce({cond}, true)) AS {name}" for name, cond in STRUCTURE_CHECKS]
    cols.append("  COUNT(*) - COUNT(DISTINCT sequence_id) AS duplicate_sequence_ids")
    return "SELECT\n" + ",\n".join(cols) + f"\nFROM {DB}.{table}" + (f"\nWHERE {where}" if where else "")


def stored_count_query(table, ds, d):
    """Stored pairs/triples for one dataset-day versus a fresh recount from transactions_clean."""
    return ("WITH\n" + ",\n".join(chain_ctes(ds, 3, (d, d))) + "\n"
            f"SELECT '{ds}' AS dataset, '{d}' AS start_date,\n"
            "  (SELECT COUNT(*) FROM pairs) AS expected_pairs,\n"
            f"  (SELECT COUNT(*) FROM {DB}.{table} WHERE dataset = '{ds}' AND start_date = '{d}' AND sequence_length = 2) AS actual_pairs,\n"
            "  (SELECT COUNT(*) FROM chains) AS expected_triples,\n"
            f"  (SELECT COUNT(*) FROM {DB}.{table} WHERE dataset = '{ds}' AND start_date = '{d}' AND sequence_length = 3) AS actual_triples")


def day_lineage_query(table, ds, d):
    """One pass over the day's chain members. Each member is joined to its own source row and to the
    source row of the next member in its chain (LEFT JOINs), so no member-to-member self-join is needed.
    missing_members: member id not in transactions_clean for that dataset and window.
    attribute_mismatches: stored timestamp, method, currencies or label differ from the source row.
    broken_links: next member does not leave the account this member paid into, within (0, 24h]."""
    src = (f"SELECT txn_id, txn_ts, from_bank, from_account, to_bank, to_account, payment_format,\n"
           f"         payment_currency, receiving_currency, is_laundering\n"
           f"  FROM {SRC} WHERE dataset = '{ds}' AND txn_date BETWEEN '{d}' AND '{shift(d, 2)}'")
    return ("WITH\n"
            f"src AS (\n  {src}\n),\n"
            "m AS (\n  SELECT u.id, u.pos, element_at(s.txn_ids, u.pos + 1) AS next_id,\n"
            "         element_at(s.txn_timestamps, u.pos) AS ts, element_at(s.payment_methods, u.pos) AS pm,\n"
            "         element_at(s.payment_currencies, u.pos) AS pc, element_at(s.receiving_currencies, u.pos) AS rc,\n"
            "         element_at(s.txn_laundering_labels, u.pos) AS lbl\n"
            f"  FROM {DB}.{table} s CROSS JOIN UNNEST(s.txn_ids) WITH ORDINALITY AS u(id, pos)\n"
            f"  WHERE s.dataset = '{ds}' AND s.start_date = '{d}'\n)\n"
            f"SELECT '{ds}' AS dataset, '{d}' AS start_date,\n"
            "  COUNT(*) AS members_checked,\n"
            "  coalesce(COUNT_IF(t.txn_id IS NULL), 0) AS missing_members,\n"
            "  coalesce(COUNT_IF(t.txn_id IS NOT NULL AND NOT coalesce(t.txn_ts = m.ts AND t.payment_format = m.pm\n"
            "      AND t.payment_currency = m.pc AND t.receiving_currency = m.rc AND t.is_laundering = m.lbl, false)), 0) AS attribute_mismatches,\n"
            "  coalesce(COUNT_IF(m.next_id IS NOT NULL AND NOT coalesce(n.from_bank = t.to_bank AND n.from_account = t.to_account\n"
            "      AND n.txn_ts > t.txn_ts AND n.txn_ts <= t.txn_ts + INTERVAL '1' DAY, false)), 0) AS broken_links\n"
            "FROM m\nLEFT JOIN src t ON t.txn_id = m.id\nLEFT JOIN src n ON n.txn_id = m.next_id")


# Source contract for the full build: exact clean row counts and owner-date coverage per dataset.
# If the ETL rules change on purpose, update these numbers in the same commit.
SOURCE_EXPECTED = {
    "HI-Medium": {"rows": 31898218, "first": "2022-09-01", "last": "2022-09-28", "days": 28},
    "LI-Medium": {"rows": 31251469, "first": "2022-09-01", "last": "2022-09-27", "days": 27},
}
# Identity of the exact files the build reads. Changes whenever the ETL rewrites a partition.
FINGERPRINT_EXPR = ("lower(to_hex(sha256(to_utf8(array_join(array_sort(array_agg(DISTINCT \"$path\" || ':' || "
                    "CAST(\"$file_size\" AS varchar) || ':' || CAST(\"$file_modified_time\" AS varchar))), '|')))))")


def source_check_query(fingerprint_expr=FINGERPRINT_EXPR):
    """One row: source_ok ('true'/'false'), detail (what was found), source_fingerprint.
    Completeness: exact row count, unique txn_id, exact first/last date and every day present."""
    parts, conds = [], []
    for ds, e in SOURCE_EXPECTED.items():
        k = "hi" if ds.startswith("HI") else "li"
        parts += [f"  COUNT_IF(dataset = '{ds}') AS {k}_rows",
                  f"  COUNT(DISTINCT CASE WHEN dataset = '{ds}' THEN txn_id END) AS {k}_ids",
                  f"  COUNT(DISTINCT CASE WHEN dataset = '{ds}' THEN txn_date END) AS {k}_days",
                  f"  min(CASE WHEN dataset = '{ds}' THEN txn_date END) AS {k}_first",
                  f"  max(CASE WHEN dataset = '{ds}' THEN txn_date END) AS {k}_last"]
        conds += [f"{k}_rows = {e['rows']}", f"{k}_ids = {e['rows']}", f"{k}_days = {e['days']}",
                  f"{k}_first = '{e['first']}'", f"{k}_last = '{e['last']}'"]
    detail = " || ".join(
        f"'{k.upper()}: ' || CAST({k}_rows AS varchar) || ' rows, ' || CAST({k}_ids AS varchar) || ' ids, ' || "
        f"CAST({k}_days AS varchar) || ' days ' || coalesce({k}_first, '?') || ' to ' || coalesce({k}_last, '?') || '. '"
        for k in ("hi", "li"))
    want = " ".join(f"{ds}: {e['rows']} rows, {e['days']} days {e['first']} to {e['last']}." for ds, e in SOURCE_EXPECTED.items())
    return ("WITH s AS (\nSELECT\n" + ",\n".join(parts) + f",\n  {fingerprint_expr} AS source_fingerprint\n"
            f"FROM {SRC}\nWHERE dataset IN ('HI-Medium', 'LI-Medium')\n)\n"
            "SELECT CASE WHEN " + "\n   AND ".join(conds) + " THEN 'true' ELSE 'false' END AS source_ok,\n"
            f"  'Found ' || {detail} || 'Expected {want}' AS detail,\n"
            "  source_fingerprint\nFROM s")


# ---------------------------------------------------------------------------
# Pipeline SQL (used by the control Lambda). The first line of every statement is a label
# comment, so collect_evidence.py can name each query from the Step Functions history.
# Gate queries return ok ('true'/'false') as their FIRST column; Step Functions reads only that.

VIEW = "bq2_sequences"
SLOTS = {"a": {"table": "bq2_sequences_a", "prefix": "bq2_sequences"},
         "b": {"table": "bq2_sequences_b", "prefix": "bq2_sequences_b"}}


def all_days():
    return [{"dataset": ds, "day": d} for ds, (f, t) in DATE_RANGES.items() for d in dates(f, t)]


def create_empty_sql(table, prefix):
    ds = next(iter(DATE_RANGES))
    d = DATE_RANGES[ds][0]
    return f"-- bq2 create {table}\n" + ctas(table, prefix, ds, 2, (d, d)) + "\nWITH NO DATA"


def build_sql(table, ds, d, length):
    return f"-- bq2 build {ds} {d} {'pairs' if length == 2 else 'triples'}\n" + insert(table, ds, length, (d, d))


def day_count_gate_query(table, ds, d):
    return (f"-- bq2 gate count {ds} {d}\n"
            "SELECT CASE WHEN c.expected_pairs = c.actual_pairs AND c.expected_triples = c.actual_triples\n"
            "   THEN 'true' ELSE 'false' END AS ok, c.*\nFROM (\n" + stored_count_query(table, ds, d) + "\n) c")


def day_lineage_gate_query(table, ds, d):
    """A day with no chains passes here with members_checked = 0; the count gate decides whether
    zero is correct (expected = actual = 0, as on the last, sparse days of a dataset)."""
    return (f"-- bq2 gate lineage {ds} {d}\n"
            "SELECT CASE WHEN c.missing_members = 0 AND c.attribute_mismatches = 0\n"
            "   AND c.broken_links = 0 THEN 'true' ELSE 'false' END AS ok, c.*\nFROM (\n"
            + day_lineage_query(table, ds, d) + "\n) c")


def structure_gate_query(table):
    names = [n for n, _ in STRUCTURE_CHECKS] + ["duplicate_sequence_ids"]
    inner = structure_query(table).replace("SELECT\n", "SELECT\n  COUNT(*) AS total_rows,\n", 1)
    return (f"-- bq2 gate structure {table}\n"
            "SELECT CASE WHEN c.total_rows > 0 AND " + " AND ".join(f"c.{n} = 0" for n in names)
            + " THEN 'true' ELSE 'false' END AS ok, c.*\nFROM (\n" + inner + "\n) c")


def source_gate_query(when):
    return f"-- bq2 gate source {when}\n" + source_check_query()


def publish_sql(table):
    return f"-- bq2 publish {table}\nCREATE OR REPLACE VIEW {DB}.{VIEW} AS SELECT * FROM {DB}.{table}"


HEADER = "-- GENERATED by generate_sql.py. Edit the generator, not this file.\n-- Run each statement separately in Athena (database is459_g2t1_aml, workgroup primary).\n\n"

DATE_RANGES = {
    "HI-Medium": ("2022-09-01", "2022-09-28"),
    "LI-Medium": ("2022-09-01", "2022-09-27"),
}


def main():
    files = {}
    files["00_preflight.sql"] = HEADER + "\n\n".join([
        "-- 1. Source IDs must be unique per dataset (duplicates would duplicate chains).\n"
        f"SELECT dataset, COUNT(*) AS rows_total, COUNT(DISTINCT txn_id) AS distinct_ids,\n"
        f"       COUNT(*) - COUNT(DISTINCT txn_id) AS duplicate_ids\nFROM {SRC}\n"
        "WHERE dataset IN ('sample', 'HI-Medium', 'LI-Medium')\nGROUP BY dataset ORDER BY dataset;",
        "-- 2. Expected output size for the sample (Ji Ning's accepted dev table: 184,449 pairs, 33,931 triples on 2022-09-01).\n"
        + size_query("sample") + ";",
        "-- 3. One full day of HI against Ji Ning's Glue job counts for owner date 2022-09-01.\n"
        "-- Expect pairs = 14679196 and triples = 36965485.\n" + day_count_query("HI-Medium", "2022-09-01") + ";",
    ])
    files["00_preflight.sql"] += ("\n\n-- 4. Source contract enforced by the pipeline and by run_athena.py before and after every build.\n"
                                  "-- source_ok must be true.\n" + source_check_query() + ";")
    files["01_validate_on_sample.sql"] = HEADER + "\n\n".join([
        "-- Rebuild the sample with this SQL into a scratch table and compare it with Ji Ning's\n"
        "-- accepted bq2_sequences_dev. All four differences must be 0.\n"
        "-- Folder s3://is459-g2t1-aml-processed-bucket/bq2_sequences_check/ must be empty first.\n"
        "DROP TABLE IF EXISTS is459_g2t1_aml.bq2_sequences_check;",
        ctas("bq2_sequences_check", "bq2_sequences_check", "sample", 2) + ";",
        insert("bq2_sequences_check", "sample", 3) + ";",
        "-- Compare every column, both directions, for the accepted owner date.\n"
        "SELECT\n"
        "  (SELECT COUNT(*) FROM (SELECT * FROM is459_g2t1_aml.bq2_sequences_check WHERE start_date = '2022-09-01'\n"
        "                         EXCEPT SELECT * FROM is459_g2t1_aml.bq2_sequences_dev)) AS only_in_new,\n"
        "  (SELECT COUNT(*) FROM (SELECT * FROM is459_g2t1_aml.bq2_sequences_dev\n"
        "                         EXCEPT SELECT * FROM is459_g2t1_aml.bq2_sequences_check WHERE start_date = '2022-09-01')) AS only_in_dev,\n"
        "  (SELECT COUNT(*) FROM is459_g2t1_aml.bq2_sequences_check WHERE start_date = '2022-09-01' AND sequence_length = 2) AS new_pairs,\n"
        "  (SELECT COUNT(*) FROM is459_g2t1_aml.bq2_sequences_check WHERE start_date = '2022-09-01' AND sequence_length = 3) AS new_triples;",
    ])
    files["03_acceptance.sql"] = HEADER + "\n\n".join([
        "-- 1. Counts by dataset and length.\n"
        f"SELECT dataset, sequence_length, COUNT(*) AS sequences, SUM(any_laundering) AS with_laundering\n"
        f"FROM {DB}.bq2_sequences GROUP BY dataset, sequence_length ORDER BY dataset, sequence_length;",
        "-- 2. Every column of every row is consistent with the v1 rules. ok must be true, every count 0.\n"
        + structure_gate_query("bq2_sequences") + ";",
    ])
    days = ["-- One statement per dataset-day: completeness (rows stored = chains recounted from source)\n"
            "-- and lineage (every member exists in transactions_clean, every stored attribute matches the\n"
            "-- source row, every link is account to account within 24h). Expected: actual = expected and\n"
            "-- missing_members, attribute_mismatches, broken_links all 0, so ok = true. The pipeline runs these\n"
            "-- same checks as a gate before publishing. Use this file for a manual recheck of the live view."]
    for ds, (d_from, d_to) in DATE_RANGES.items():
        for d in dates(d_from, d_to):
            days.append(day_count_gate_query("bq2_sequences", ds, d) + ";")
            days.append(day_lineage_gate_query("bq2_sequences", ds, d) + ";")
    files["04_day_checks.sql"] = HEADER + "\n\n".join(days)
    for name, sql in files.items():
        (HERE / name).write_text(sql + "\n", encoding="utf-8")
        print("wrote", name, sql.count(";"), "statements")
    return files


if __name__ == "__main__":
    main()
