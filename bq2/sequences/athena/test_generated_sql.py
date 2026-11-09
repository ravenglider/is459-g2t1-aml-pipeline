"""Local test: run the generated Athena SQL (transpiled to DuckDB) on synthetic data and
compare every output row with a brute-force Python implementation of sequence_rules.md.

    pip install sqlglot duckdb
    python3 test_generated_sql.py
"""
import hashlib
import random
from datetime import datetime, timedelta

import duckdb
import sqlglot

import generate_sql as g

random.seed(459)
BASE = datetime(2022, 9, 1, 0, 0)


def make_rows(ds, n=1500):
    accts = [(b, f"{a:08X}") for b in (1, 2, 3) for a in range(6)]  # few accounts -> busy hubs
    rows, seen = [], set()
    for _ in range(n):
        fb, fa = random.choice(accts)
        tb_, ta_ = random.choice(accts)
        minute = random.choice([random.randint(0, 4 * 1440), 1440, 0, 2 * 1440])  # exact-24h/equal-ts edges
        ts = BASE + timedelta(minutes=minute)
        tid = random.randint(-2**63, 2**63 - 1)
        if tid in seen:
            continue
        seen.add(tid)
        rows.append(dict(txn_id=tid, txn_ts=ts, from_bank=fb, from_account=fa, to_bank=tb_, to_account=ta_,
                         payment_format=random.choice(["Wire", "ACH", "Cash", "Cheque", "Bitcoin"]),
                         payment_currency=random.choice(["US Dollar", "Euro", "Bitcoin"]),
                         receiving_currency=random.choice(["US Dollar", "Euro", "UK Pound"]),
                         is_laundering=int(random.random() < 0.2), dataset=ds,
                         txn_date=ts.strftime("%Y-%m-%d")))
    return rows


def linked(a, b):
    return (a["to_bank"] == b["from_bank"] and a["to_account"] == b["from_account"]
            and a["txn_ts"] < b["txn_ts"] <= a["txn_ts"] + timedelta(days=1) and a["txn_id"] != b["txn_id"])


def reference(rows, ds):
    out = set()
    for a in rows:
        for b in rows:
            if not linked(a, b):
                continue
            out.add(row_key(ds, [a, b]))
            for c in rows:
                if linked(b, c) and c["txn_id"] not in (a["txn_id"], b["txn_id"]):
                    out.add(row_key(ds, [a, b, c]))
    return out


def row_key(ds, ch):
    ids = [t["txn_id"] for t in ch]
    labels = [t["is_laundering"] for t in ch]
    sid = hashlib.sha256(("v1\n" + ds + "\n" + ",".join(str(i) for i in ids)).encode()).hexdigest()
    gaps = tuple(int((ch[i + 1]["txn_ts"] - ch[i]["txn_ts"]).total_seconds()) for i in range(len(ch) - 1))
    return (sid, "v1", len(ch), tuple(ids), tuple(t["txn_ts"] for t in ch),
            tuple(t["payment_format"] for t in ch), " -> ".join(t["payment_format"] for t in ch),
            tuple(t["payment_currency"] for t in ch), tuple(t["receiving_currency"] for t in ch),
            " -> ".join(t["payment_currency"] for t in ch), " -> ".join(t["receiving_currency"] for t in ch),
            tuple(labels), sum(labels), int(sum(labels) > 0), int(sum(labels) == len(ch)),
            ids[-1], labels[-1], gaps, int((ch[-1]["txn_ts"] - ch[0]["txn_ts"]).total_seconds()),
            ds, ch[0]["txn_ts"].strftime("%Y-%m-%d"))


def to_duckdb(sql):
    return sqlglot.transpile(sql, read="trino", write="duckdb")[0]


def run(con, ds, length, window=None):
    sql = to_duckdb(g.body(ds, length, window))
    res = con.execute(sql).fetchall()
    return {tuple(tuple(v) if isinstance(v, list) else v for v in r) for r in res}


STRUCT_NAMES = [n for n, _ in g.STRUCTURE_CHECKS] + ["duplicate_sequence_ids"]
DAY = ("HI-Medium", "2022-09-01")
T = "is459_g2t1_aml.bq2_sequences"

# Each corruption must be caught by the named check(s) and by nothing that should stay 0.
CORRUPTIONS = [
    ("rule_version", f"UPDATE {T} SET rule_version = 'v0' WHERE sequence_id = (SELECT min(sequence_id) FROM {T})", ["bad_rule_version"]),
    ("any_laundering", f"UPDATE {T} SET any_laundering = 1 - any_laundering WHERE sequence_id = (SELECT min(sequence_id) FROM {T})", ["bad_any_laundering"]),
    ("all_laundering", f"UPDATE {T} SET all_laundering = 1 - all_laundering WHERE sequence_id = (SELECT min(sequence_id) FROM {T})", ["bad_all_laundering"]),
    ("final_is_laundering", f"UPDATE {T} SET final_is_laundering = 1 - final_is_laundering WHERE sequence_id = (SELECT min(sequence_id) FROM {T})", ["bad_final_label"]),
    ("laundering_count", f"UPDATE {T} SET laundering_count = laundering_count + 1 WHERE sequence_id = (SELECT min(sequence_id) FROM {T})", ["bad_label_count"]),
    ("duration", f"UPDATE {T} SET duration_seconds = duration_seconds + 1 WHERE sequence_id = (SELECT min(sequence_id) FROM {T})", ["bad_duration"]),
    ("gap value", f"UPDATE {T} SET gap_seconds = list_transform(gap_seconds, lambda x: x + 1) WHERE sequence_id = (SELECT min(sequence_id) FROM {T})", ["bad_gap_values"]),
    ("currency string", f"UPDATE {T} SET receiving_currency_sequence = 'X' WHERE sequence_id = (SELECT min(sequence_id) FROM {T})", ["bad_currency_strings"]),
    ("method string", f"UPDATE {T} SET method_sequence = 'X' WHERE sequence_id = (SELECT min(sequence_id) FROM {T})", ["bad_method_string"]),
    ("start_date", f"UPDATE {T} SET start_date = '2022-09-02' WHERE sequence_id = (SELECT min(sequence_id) FROM {T} WHERE start_date = '2022-09-01')", ["bad_start_date"]),
    ("sequence_id", f"UPDATE {T} SET sequence_id = 'x' || sequence_id WHERE sequence_id = (SELECT min(sequence_id) FROM {T})", ["bad_sequence_id"]),
    ("duplicate row", f"INSERT INTO {T} SELECT * FROM {T} WHERE sequence_id = (SELECT min(sequence_id) FROM {T} WHERE start_date = '2022-09-01')", ["duplicate_sequence_ids", "count"]),
    ("dropped row", f"DELETE FROM {T} WHERE sequence_id = (SELECT min(sequence_id) FROM {T} WHERE start_date = '2022-09-01')", ["count"]),
    ("missing source member", "DELETE FROM is459_g2t1_aml.transactions_clean WHERE txn_id = (SELECT max(txn_ids[1]) FROM "
     f"{T} WHERE start_date = '2022-09-01')", ["missing_members", "count"]),
    ("stored attribute", f"UPDATE {T} SET payment_methods = list_transform(payment_methods, lambda x: 'X') WHERE sequence_id = "
     f"(SELECT min(sequence_id) FROM {T} WHERE start_date = '2022-09-01')", ["attribute_mismatches", "bad_method_string"]),
]


def acceptance_checks(make_con):
    """The acceptance SQL must return all zeros on correct data and catch every corruption."""
    def results(con):
        """Runs the pipeline's gate queries. flags = what was detected; flags['gate'] = the two ok values."""
        srow = con.execute(to_duckdb(g.structure_gate_query("bq2_sequences"))).fetchone()
        crow = con.execute(to_duckdb(g.day_count_gate_query("bq2_sequences", *DAY))).fetchone()
        lrow = con.execute(to_duckdb(g.day_lineage_gate_query("bq2_sequences", *DAY))).fetchone()
        struct = dict(zip(STRUCT_NAMES, srow[2:]))
        exp_p, act_p, exp_t, act_t = crow[3:7]
        miss, attr, broken = lrow[4:7]
        flags = {k: v for k, v in struct.items() if v}
        if (exp_p, exp_t) != (act_p, act_t):
            flags["count"] = 1
        for k, v in (("missing_members", miss), ("attribute_mismatches", attr), ("broken_links", broken)):
            if v:
                flags[k] = v
        gates = (srow[0], crow[0], lrow[0])
        if flags and "false" not in gates:
            flags["GATE_DID_NOT_BLOCK"] = gates
        if not flags and gates != ("true", "true", "true"):
            flags["GATE_FALSE_ALARM"] = gates
        return flags
    ok = True
    clean = results(make_con())
    print(f"   acceptance on correct data: {'all 0, all three gates ok = true' if not clean else clean}")
    ok &= not clean
    for name, sql, expect in CORRUPTIONS:
        con = make_con()
        con.execute(sql)
        flags = results(con)
        hit = all(e in flags for e in expect) and "GATE_DID_NOT_BLOCK" not in flags
        print(f"   corruption '{name}': caught by {sorted(flags)} {'OK' if hit else 'MISSED, expected ' + str(expect)}")
        ok &= hit
    # a day with no transactions at all: both day gates must pass (0 expected, 0 stored),
    # and a missing-but-expected day must fail the count gate
    con = make_con()
    empty = (con.execute(to_duckdb(g.day_count_gate_query("bq2_sequences", "HI-Medium", "2022-09-06"))).fetchone()[0],
             con.execute(to_duckdb(g.day_lineage_gate_query("bq2_sequences", "HI-Medium", "2022-09-06"))).fetchone()[0])
    con.execute(f"DELETE FROM {T} WHERE start_date = '2022-09-02'")
    lost = con.execute(to_duckdb(g.day_count_gate_query("bq2_sequences", "HI-Medium", "2022-09-02"))).fetchone()[0]
    print(f"   empty day passes both gates: {empty == ('true', 'true')}; a day with chains but nothing stored fails: {lost == 'false'}")
    ok &= empty == ("true", "true") and lost == "false"
    # a broken link: replace the second member of a pair with an unrelated transaction, attributes copied
    con = make_con()
    con.execute(f"""UPDATE {T} s SET txn_ids = [s.txn_ids[1], t.txn_id], txn_timestamps = [s.txn_timestamps[1], t.txn_ts],
        payment_methods = [s.payment_methods[1], t.payment_format], payment_currencies = [s.payment_currencies[1], t.payment_currency],
        receiving_currencies = [s.receiving_currencies[1], t.receiving_currency],
        txn_laundering_labels = [s.txn_laundering_labels[1], t.is_laundering]
        FROM (SELECT * FROM is459_g2t1_aml.transactions_clean WHERE txn_date = '2022-09-01' ORDER BY txn_ts DESC LIMIT 1) t
        WHERE s.sequence_id = (SELECT min(sequence_id) FROM {T} WHERE start_date = '2022-09-01' AND sequence_length = 2)""")
    flags = results(con)
    print(f"   corruption 'broken link': caught by {sorted(flags)} {'OK' if 'broken_links' in flags else 'MISSED'}")
    ok &= "broken_links" in flags and "GATE_DID_NOT_BLOCK" not in flags
    return ok


def main():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA is459_g2t1_aml")
    rows = make_rows("HI-Medium") + make_rows("LI-Medium", 600)
    con.execute("""CREATE TABLE is459_g2t1_aml.transactions_clean (txn_id BIGINT, txn_ts TIMESTAMP, from_bank BIGINT,
        from_account VARCHAR, to_bank BIGINT, to_account VARCHAR, payment_format VARCHAR, payment_currency VARCHAR,
        receiving_currency VARCHAR, is_laundering INTEGER, dataset VARCHAR, txn_date VARCHAR)""")
    cols = list(rows[0])
    con.executemany(f"INSERT INTO is459_g2t1_aml.transactions_clean VALUES ({','.join('?' * len(cols))})",
                    [[r[c] for c in cols] for r in rows])
    ok = True
    for ds in ("HI-Medium", "LI-Medium"):
        ds_rows = [r for r in rows if r["dataset"] == ds]
        ref = reference(ds_rows, ds)
        got = run(con, ds, 2) | run(con, ds, 3)
        print(f"{ds}: reference {len(ref)} chains ({sum(1 for r in ref if r[2]==2)} pairs, "
              f"{sum(1 for r in ref if r[2]==3)} triples); SQL {len(got)}; match={ref == got}")
        ok &= ref == got
        # the daily build (one start date per statement) must give exactly the same rows
        days = sorted({r["txn_date"] for r in ds_rows})
        win = set()
        for d in days:
            win |= run(con, ds, 2, (d, d)) | run(con, ds, 3, (d, d))
        print(f"   daily build over {len(days)} days match={win == got}")
        ok &= win == got
        # the one-day count query must agree with the rows built for that day
        d0 = days[0]
        cnt = con.execute(to_duckdb(g.day_count_query(ds, d0))).fetchone()
        exp = (sum(1 for r in got if r[2] == 2 and r[-1] == d0), sum(1 for r in got if r[2] == 3 and r[-1] == d0))
        print(f"   day count {d0}: {cnt} expected {exp} match={tuple(cnt) == exp}")
        ok &= tuple(cnt) == exp
    # acceptance SQL catches corruptions
    hi_rows = [r for r in rows if r["dataset"] == "HI-Medium"]
    def make_con():
        c = duckdb.connect()
        c.execute("CREATE SCHEMA is459_g2t1_aml")
        c.execute("""CREATE TABLE is459_g2t1_aml.transactions_clean (txn_id BIGINT, txn_ts TIMESTAMP, from_bank BIGINT,
            from_account VARCHAR, to_bank BIGINT, to_account VARCHAR, payment_format VARCHAR, payment_currency VARCHAR,
            receiving_currency VARCHAR, is_laundering INTEGER, dataset VARCHAR, txn_date VARCHAR)""")
        c.executemany(f"INSERT INTO is459_g2t1_aml.transactions_clean VALUES ({','.join('?' * len(cols))})",
                      [[r[k] for k in cols] for r in hi_rows])
        c.execute("CREATE TABLE is459_g2t1_aml.bq2_sequences AS " + to_duckdb(g.body("HI-Medium", 2)))
        c.execute("INSERT INTO is459_g2t1_aml.bq2_sequences " + to_duckdb(g.body("HI-Medium", 3)))
        return c
    print("acceptance checks:")
    ok &= acceptance_checks(make_con)
    # hash spec check from schema_bq2_sequences.md
    spec = con.execute(to_duckdb("SELECT lower(to_hex(sha256(to_utf8('v1' || chr(10) || 'sample' || chr(10) || '101,102,103'))))")).fetchone()[0]
    print("hash spec match:", spec == "d7ccc2d78595f228b8c0957d1b22ebf1cac5d78e4b294f4c09a809c9861c7a30")
    ok &= spec == "d7ccc2d78595f228b8c0957d1b22ebf1cac5d78e4b294f4c09a809c9861c7a30"
    # all generated statements must parse as Trino
    for f in ["00_preflight.sql", "01_validate_on_sample.sql", "03_acceptance.sql", "04_day_checks.sql"]:
        for stmt in [s for s in open(f).read().split(";") if s.strip() and not all(l.startswith("--") or not l.strip() for l in s.splitlines())]:
            sqlglot.parse_one(stmt, read="trino")
    # every statement the pipeline sends (via the control Lambda) must parse as Trino
    for slot in g.SLOTS.values():
        t = slot["table"]
        pipeline_sql = [g.create_empty_sql(t, slot["prefix"]).replace("\nWITH NO DATA", ""), g.structure_gate_query(t),
                        g.source_gate_query("before build"), g.publish_sql(t)]
        for item in g.all_days():
            pipeline_sql += [g.build_sql(t, item["dataset"], item["day"], 2), g.build_sql(t, item["dataset"], item["day"], 3),
                             g.day_count_gate_query(t, item["dataset"], item["day"]),
                             g.day_lineage_gate_query(t, item["dataset"], item["day"])]
        for stmt in pipeline_sql:
            sqlglot.parse_one(stmt, read="trino")
    print("all statements parse as Trino SQL (files and pipeline)")
    # the lineage gate must read the day's stored rows once and never join members to members
    lq = g.day_lineage_gate_query("bq2_sequences_b", "HI-Medium", "2022-09-02")
    light = lq.count("FROM is459_g2t1_aml.bq2_sequences_b") == 1 and " m x" not in lq and "JOIN m" not in lq \
        and lq.count("JOIN src") == 2 and "(SELECT" not in lq.split("FROM (", 1)[1]
    print(f"lineage gate is a single pass (one read of the day, no self-join): {light}")
    ok &= light
    print("ALL PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
