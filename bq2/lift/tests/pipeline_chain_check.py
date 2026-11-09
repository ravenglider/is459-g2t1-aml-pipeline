"""Run the exact Role 6 statements embedded in the ASL, in order, on synthetic data in DuckDB,
then the lift gate. Then corrupt outputs and check the gate fails."""
import json, random, re, sys
import duckdb
from pathlib import Path
DEFAULT = Path(__file__).resolve().parents[1] / "lift_only.asl.json"
asl = json.load(open(sys.argv[1] if len(sys.argv) > 1 else DEFAULT))
st = asl["States"]
order, cur = [], asl["StartAt"]
while cur:
    s = st[cur]; order.append(cur); cur = s.get("Next")
    if s["Type"] == "Choice": cur = s["Choices"][0]["Next"]
sqls = [(n, st[n]["Parameters"]["QueryString"]) for n in order if "QueryString" in st[n].get("Parameters", {})]
print("statements:", [n for n, _ in sqls])

def duck(sql):
    return re.sub(r"WITH \(\s*format = 'PARQUET',\s*external_location = '[^']+'\s*\) AS", "AS", sql)

def fresh():
    con = duckdb.connect(); con.execute("CREATE SCHEMA is459_g2t1_aml")
    random.seed(7); methods = ["ACH", "Wire", "Cheque", "Cash", "Credit Card", "Bitcoin"]
    txn = {}
    for ds in ("HI-Medium", "LI-Medium"):
        for i in range(30000):
            txn[(ds, i)] = 1 if random.random() < (0.05 if ds == "HI-Medium" else 0.01) else 0
    con.execute("CREATE TABLE is459_g2t1_aml.transactions_clean (dataset VARCHAR, txn_id BIGINT, is_laundering INTEGER)")
    import pandas as pd
    tdf = pd.DataFrame([(d, i, l) for (d, i), l in txn.items()], columns=["dataset", "txn_id", "is_laundering"])
    con.execute("INSERT INTO is459_g2t1_aml.transactions_clean SELECT * FROM tdf")
    rows = []
    for ds in ("HI-Medium", "LI-Medium"):
        for L in (2, 3):
            for _ in range(15000):
                ms = [random.choice(methods[:4] if random.random() < .8 else methods) for _ in range(L)]
                fin = random.randrange(30000)
                lab = txn[(ds, fin)] | (1 if random.random() < (0.3 if ms[0] == "Bitcoin" else 0.02) else 0)
                rows.append((ds, " -> ".join(ms), L, lab, fin))
    con.execute("CREATE TABLE is459_g2t1_aml.bq2_sequences (dataset VARCHAR, method_sequence VARCHAR, sequence_length INTEGER, any_laundering INTEGER, final_txn_id BIGINT)")
    sdf = pd.DataFrame(rows, columns=["dataset", "method_sequence", "sequence_length", "any_laundering", "final_txn_id"])
    con.execute("INSERT INTO is459_g2t1_aml.bq2_sequences SELECT * FROM sdf")
    for n, q in sqls:
        con.execute(duck(q))
    return con

def gate(con):
    cur = con.execute(sqls[-1][1]); cols = [c[0] for c in cur.description]
    return dict(zip(cols, cur.fetchone()))

con = fresh(); g = gate(con)
print({k: g[k] for k in g}); assert g["ok"] == "true", g
print("tradeoff rows", con.execute("select count(*) from is459_g2t1_aml.bq2_tradeoff").fetchone())

corruptions = {
  "insert appended twice (retry)": "INSERT INTO is459_g2t1_aml.bq2_tradeoff_flagged SELECT * FROM is459_g2t1_aml.bq2_tradeoff_flagged WHERE dataset='LI-Medium' AND sequence_length=3",
  "missing LI triples insert": "DELETE FROM is459_g2t1_aml.bq2_tradeoff_flagged WHERE dataset='LI-Medium' AND sequence_length=3; DELETE FROM is459_g2t1_aml.bq2_lift WHERE false",
  "lift row lost": "DELETE FROM is459_g2t1_aml.bq2_lift WHERE rowid = (SELECT min(rowid) FROM is459_g2t1_aml.bq2_lift WHERE dataset='HI-Medium' AND sequence_length=2 AND min_support=100)",
  "tradeoff row lost": "DELETE FROM is459_g2t1_aml.bq2_tradeoff WHERE rowid = 0",
  "tradeoff accounting": "UPDATE is459_g2t1_aml.bq2_tradeoff SET reviewed = reviewed + 1 WHERE policy='Targeted top-3' AND dataset='HI-Medium' AND sequence_length=2 AND min_support=50",
  "robustness row lost": "DELETE FROM is459_g2t1_aml.bq2_robustness WHERE rowid = 0",
  "sankey row lost": "DELETE FROM is459_g2t1_aml.bq2_sankey WHERE rowid = 0",
  "sequences changed after lift": "INSERT INTO is459_g2t1_aml.bq2_sequences VALUES ('HI-Medium','ACH -> Wire',2,0,1)",
  "bad rank": "UPDATE is459_g2t1_aml.bq2_tradeoff_flagged SET best_rank = 11 WHERE rowid = 0",
}
for name, sql in corruptions.items():
    con = fresh(); con.execute(sql); g = gate(con)
    print(("BLOCKED " if g["ok"] == "false" else "MISSED  ") + name)
    assert g["ok"] == "false", (name, g)
print("ALL PASS")
