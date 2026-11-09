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
    # hash spec check from schema_bq2_sequences.md
    spec = con.execute(to_duckdb("SELECT lower(to_hex(sha256(to_utf8('v1' || chr(10) || 'sample' || chr(10) || '101,102,103'))))")).fetchone()[0]
    print("hash spec match:", spec == "d7ccc2d78595f228b8c0957d1b22ebf1cac5d78e4b294f4c09a809c9861c7a30")
    ok &= spec == "d7ccc2d78595f228b8c0957d1b22ebf1cac5d78e4b294f4c09a809c9861c7a30"
    # all generated statements must parse as Trino
    for f in ["00_preflight.sql", "01_validate_on_sample.sql", "02_build_daily.sql", "03_acceptance.sql"]:
        for stmt in [s for s in open(f).read().split(";") if s.strip() and not all(l.startswith("--") or not l.strip() for l in s.splitlines())]:
            sqlglot.parse_one(stmt, read="trino")
    print("all statements parse as Trino SQL")
    print("ALL PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
