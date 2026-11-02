# viz: Tableau dashboards

**Owner:** Roles 3 and 6 (name: ____________)

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

_2 to 3 lines._

### AWS resources

| Resource | Exact name |
|---|---|
| | |

### Inputs and outputs

| | Table | S3 path |
|---|---|---|
| Reads | | |
| Writes | | |

### How to run

1. 
2. 

### Step Functions info for Sonia

| Field | Value |
|---|---|
| Job type | |
| Exact name | |
| Output table | |
| Output S3 path | |
| Depends on | |
| Typical run time on full data | |

### Contribution summary (for the Week 13 slide)

_1 to 2 lines._

## Role 6 progress — Xiang Ying

BQ2 SQL inputs and [Tableau build guide with a four-sheet layout specification](bq2_tableau.md#four-sheet-layout-prepared-before-role-5) are prepared. The layout specifies positions, fields, units and filter behaviour with findings left blank; no native workbook is claimed. Proposed sources: `bq2_lift`, `bq2_tradeoff`, `bq2_robustness`, `bq2_sankey` in `is459_g2t1_aml`. They have not been deployed by this work. Role 5's confirmed HI/LI chains and transaction membership are required before real-data visuals. BQ2 workbook/screenshots and measured results remain pending. Update this section after execution and push the completion evidence.
