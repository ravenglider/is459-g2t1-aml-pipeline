# viz: Tableau dashboards

**Owner:** Roles 3 and 6 (name: Ryan, Xiang Ying)

Tableau dashboards for BQ1 (heatmap, ranked corridors) and BQ2 (Sankey, trade-off chart, targeted check list), connected to the final Athena tables.

## What to commit here

| File | Contents |
|---|---|
| `bq1_dashboard.twb` | BQ1 workbook (`.twb`, not `.twbx`) |
| `bq2_dashboard.twb` | BQ2 workbook |
| `screenshots/` | PNG of each dashboard for the slides |
| `connection.md` | How to connect Tableau to Athena (driver, region, S3 staging directory, workgroup) |

## Notes for this role

- Commit `.twb` only: `.twbx` and `.hyper` embed data and are blocked by `.gitignore`.
- Never save AWS keys inside the workbook or `connection.md`.
- Keep colours and fonts consistent across both dashboards.

---

## README template (fill in as you build)

### What it does

Tableau dashboards visualizing BQ1 (bank-pair corridor heatmap and a ranked top-20 list of highest-risk corridors by extra laundering value) and BQ2 results, connected live to the final Athena tables rather than static CSV exports.

### AWS resources

| Resource | Exact name |
|---|---|
| Athena workgroup | primary |
| S3 staging directory | `s3://is459-g2t1-aml-athena-bucket/results/` |
| Glue database | `is459_g2t1_aml` |
| IAM user | `g2team1` |

### Inputs and outputs

| | Table | S3 path |
|---|---|---|
| Reads (BQ1) | `bq1_corridors_ranked` | `s3://is459-g2t1-aml-processed-bucket/bq1_corridors_ranked/` |
| Reads (BQ2) | `bq2_lift`, `bq2_tradeoff`, `bq2_robustness`, `bq2_sankey` | *pending — Role 5/Role 6* |
| Writes | *(none — dashboard only, no tables produced)* | |

### How to run

1. Open `bq1_dashboard.twb` in Tableau Desktop. You'll need the Simba Athena JDBC driver installed locally and your own AWS Access Key ID/Secret Access Key for the `g2team1` account — see `connection.md`.
2. The dashboard pulls live from Athena on open — no manual data load or refresh needed.

### Step Functions info for Sonia

| Field | Value |
|---|---|
| Job type | N/A — manual visualization step, not part of the automated Step Function pipeline |

### Contribution summary (for the Week 13 slide)

Built BQ1's live-connected heatmap and top-20 ranked corridor table in Tableau; identified and resolved a data-quality outlier issue (billion/trillion-dollar phantom transactions) that was distorting the rankings.

## Role 6 progress — Xiang Ying

BQ2 SQL inputs and [Tableau build guide with a four-sheet layout specification](bq2_tableau.md#four-sheet-layout-prepared-before-role-5) are prepared. The layout specifies positions, fields, units and filter behaviour with findings left blank; no native workbook is claimed. Proposed sources: `bq2_lift`, `bq2_tradeoff`, `bq2_robustness`, `bq2_sankey` in `is459_g2t1_aml`. They have not been deployed by this work. Role 5's published v1 HI/LI chains are required before real-data visuals. Inputs use any_laundering, distinct final transactions and separate length/support policies; ordered membership is an array, not a physical table. BQ2 workbook/screenshots and measured results remain pending. Update this section after execution and push the completion evidence.

## Role 3 progress — Ryan

BQ1 Athena pipeline complete: four chained tables (`bq1_direct_corridors`, `bq1_attempt_banks`, `bq1_shared_attempt_value`, `bq1_corridors_ranked`) producing Blindspot Gap and Extra Laundering Value per bank-pair corridor, with a $20M outlier filter applied after percentile analysis confirmed the dataset contains unrealistic billion/trillion-dollar transactions unrelated to actual laundering activity (confirmed with professor). Tableau connected live to Athena (not a CSV export); heatmap and top-20 ranked table built and filtered for readability. Priority sharing list complete: `bq1_priority_sharing_list` joins Role 4's (Le Xin) graph features (betweenness, community_id) onto the corridor ranking, with a cross_community_flag identifying corridors that bridge two separate bank communities — a stronger signal for information-sharing priority.
