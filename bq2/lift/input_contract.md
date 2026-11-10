# Role 6 input contract — aligned to Role 5 v1

> **Historical (10 November 2026):** this was the sample-stage interface check. The final run read the production `bq2_sequences` view; see the [Role 6 README](README.md).

Status: Role 6 implementation is aligned to Ji Ning's documented 21-column interface. Ji Ning reports successful sample publication and schema/value/lineage acceptance; those AWS results have not been independently retrieved in this session. Start with the [development runbook](development/README.md). This is an interface-readiness statement, not a claim that Role 5 has deployed a builder or that a direct message has been sent.

## Canonical interface

Use [Role 5's 21-column schema](../sequences/schema_bq2_sequences.md) and [sequence rules](../sequences/sequence_rules.md) as the authoritative definitions. Do not maintain a second conflicting schema here.

| Purpose | Table | Required handling |
|---|---|---|
| Full production occurrences | `is459_g2t1_aml.bq2_sequences` | `dataset` = HI-Medium or LI-Medium, evaluated separately by length |
| Development batch | `is459_g2t1_aml.bq2_sequences_dev` | Preserve actual source dataset, initially sample; use only development destinations |
| Original labels/denominators | `is459_g2t1_aml.transactions_clean` | Join on dataset AND bigint txn_id; full cleaned population denominators |

Sequence roots are `s3://is459-g2t1-aml-processed-bucket/bq2_sequences/` and `.../bq2_sequences_dev/`, partitioned by dataset/start_date. Ordered membership is `txn_ids array<bigint>`; no physical `bq2_sequence_transactions` table is required. Preserve signed bigint IDs exactly, including negative values; never convert through floating point.

## Fields Role 6 uses

- Lift: `dataset`, `sequence_length`, `method_sequence`, `any_laundering`.
- Targeting: same pattern/length plus `final_txn_id`. Read the original final label from clean; `final_is_laundering` is independently checked through the aligned label array.
- Sankey: readable methods split on literal ` -> `; `payment_methods` is checked as their source representation.
- Lineage: `sequence_id`, `rule_version`, `txn_ids`, `txn_timestamps`, `txn_laundering_labels`, gaps, duration and start_date.
- Secondary currencies: explicit payment/receiving arrays and strings. No generic `currency_sequence` or generic chain `is_laundering` is expected.

No extra columns are needed for lift, distinct-final targeting, LI robustness or Sankey.

## Rule alignment

2/3 transfers are separate analytical populations. Each adjacent transfer is strictly later and at most 24 hours later; tied times are excluded and triples can span up to 48h. Match both bank and account within one dataset. Retain all branches, same-bank transfers, repeated methods, cycles/self-transfers where the rules allow, and overlapping chains; prohibit repeated transactions within a chain and deduplicate exact ordered chains. No amount/currency equality is required.

Use any positive hop for sequence lift, but the final transaction's original label for checks. For `[1,0]`, the chain is positive while its final transaction is legitimate. Do not require HI-only attempt metadata. Do not mix rule versions or refreshed source snapshots in one run.

## Sample-first acceptance

1. Ji Ning publishes development table/schema, complete ordered chains, source coverage, rule/builder version and successful-completion manifest. The received handoff is 218,380 chains for sample/start_date=2022-09-01 (184,449 pairs, 33,931 triples); use that approved scope. The earlier 100–200 suggestion is superseded.
2. Include legitimate/mixed/positive chains, both lengths, repeated methods and overlapping finals where available; report missing cases instead of manufacturing labels. Confirm whether the batch is exhaustive or handpicked.
3. Use an isolated development copy of SQL/validation: replace only sequence input with `bq2_sequences_dev`, source dataset filters with the actual value (e.g. sample), and ALL output names/references/S3 destinations with consistent `_dev` counterparts. The clean source remains `transactions_clean`, with the same sample dataset. Never relabel sample as HI or create development output in production prefixes. The HI-to-LI robustness query is not meaningful for a sample-only batch; skip it until both full datasets exist.
4. Run the clean-source uniqueness check, sequence preflight, label/string/array, lineage and link validations. Inspect at least one traced real chain. Handpicked-batch ranks/prevalence are not findings; support cutoffs may yield no eligible patterns and must not be lowered to invent a winner.
5. Only after sample acceptance and full publication: inspect HI frequencies without outcomes; evaluate 50/100/500 sensitivity; run full HI/LI analytics against the same validated source snapshot. Record actual schema, version and completion evidence before marking integration complete.

Still to verify: precise sequence-ID serialization/repeatability, input uniqueness/quality, upstream timestamp interpretation, growth limits, actual development batch, staged publication/recovery and synchronized HI/LI readiness. These are implementation acceptance checks, not new fields or permission to change another role's code.

## Current handoff and reply

Ji Ning reports successful publication, exact schema match, zero focused value/lineage defects and eight verified Parquet files. Her acceptance query IDs and remaining Role 6 steps are in [development/README.md](development/README.md).

> Thanks for the accepted sample handoff. I'll use bq2_sequences_dev with dataset=sample and start_date=2022-09-01 for Role 6 integration checks, keeping outputs in separate development tables. I'll reconcile the reported counts and record my own query IDs. I'll share any issues before we proceed to full HI/LI findings.

This reply is prepared, not sent. Own integration results remain pending.
