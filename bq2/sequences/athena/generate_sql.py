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
    daily = ["-- Builds the production table one start date at a time (HI 28 days, LI 27 days, pairs and\n"
             "-- triples): 111 statements. HI alone is about 50 million chains per day, too big for one\n"
             "-- statement within Athena's 30 minute limit. Run with run_athena.py, not by hand.\n"
             "-- The folder s3://is459-g2t1-aml-processed-bucket/bq2_sequences/ must be empty first.\n"
             "DROP TABLE IF EXISTS is459_g2t1_aml.bq2_sequences;"]
    first = True
    for ds, (d_from, d_to) in DATE_RANGES.items():
        for d in dates(d_from, d_to):
            for length in (2, 3):
                label = f"-- {ds} {d} {'pairs' if length == 2 else 'triples'}\n"
                if first:
                    daily.append(label + ctas("bq2_sequences", "bq2_sequences", ds, length, (d, d)) + ";")
                    first = False
                else:
                    daily.append(label + insert("bq2_sequences", ds, length, (d, d)) + ";")
    files["02_build_daily.sql"] = HEADER + "\n\n".join(daily)
    files["03_acceptance.sql"] = HEADER + "\n\n".join([
        "-- 1. Counts by dataset and length (compare with 00_preflight sizes).\n"
        f"SELECT dataset, sequence_length, COUNT(*) AS sequences, SUM(any_laundering) AS with_laundering\n"
        f"FROM {DB}.bq2_sequences GROUP BY dataset, sequence_length ORDER BY dataset, sequence_length;",
        "-- 2. Structure: every count below must be 0.\n"
        f"SELECT\n"
        "  COUNT_IF(cardinality(txn_ids) <> sequence_length) AS bad_id_arrays,\n"
        "  COUNT_IF(cardinality(gap_seconds) <> sequence_length - 1) AS bad_gap_arrays,\n"
        "  COUNT_IF(cardinality(array_distinct(txn_ids)) <> sequence_length) AS repeated_ids,\n"
        "  COUNT_IF(any_match(gap_seconds, g -> g <= 0 OR g > 86400)) AS bad_gaps,\n"
        "  COUNT_IF(final_txn_id <> element_at(txn_ids, -1)) AS bad_final_id,\n"
        "  COUNT_IF(method_sequence <> array_join(payment_methods, ' -> ')) AS bad_method_string,\n"
        "  COUNT_IF(laundering_count <> reduce(txn_laundering_labels, 0, (s, x) -> s + x, s -> s)) AS bad_label_count,\n"
        "  COUNT(*) - COUNT(DISTINCT sequence_id) AS duplicate_sequence_ids\n"
        f"FROM {DB}.bq2_sequences;",
        "-- 3. Lineage on one day per dataset (the full table is too big to re-join): every link really\n"
        "-- connects account to account within 24h. Must be 0.\n"
        f"WITH m AS (\n  SELECT s.dataset, s.sequence_id, pos, id\n  FROM {DB}.bq2_sequences s\n"
        "  CROSS JOIN UNNEST(s.txn_ids) WITH ORDINALITY AS u(id, pos)\n"
        "  WHERE s.start_date = '2022-09-01'\n),\n"
        f"j AS (\n  SELECT m.sequence_id, m.pos, t.*\n  FROM m JOIN {SRC} t ON t.dataset = m.dataset AND t.txn_id = m.id\n)\n"
        "SELECT COUNT(*) AS broken_links\nFROM j x JOIN j y ON y.sequence_id = x.sequence_id AND y.pos = x.pos + 1\n"
        "WHERE NOT (y.from_bank = x.to_bank AND y.from_account = x.to_account\n"
        "           AND y.txn_ts > x.txn_ts AND y.txn_ts <= x.txn_ts + INTERVAL '1' DAY);",
    ])
    for name, sql in files.items():
        (HERE / name).write_text(sql + "\n", encoding="utf-8")
        print("wrote", name, sql.count(";"), "statements")
    return files


if __name__ == "__main__":
    main()
