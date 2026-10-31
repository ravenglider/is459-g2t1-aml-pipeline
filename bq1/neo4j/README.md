# bq1/neo4j: Bank relationship graph

**Owner:** Role 4 (name: ____________)

Loads bank-to-bank laundering edges into Neo4j on EC2 to trace multi-hop paths, intermediary banks and clusters, visualised in Bloom.

## What to commit here

| File | Contents |
|---|---|
| `ec2_setup.md` | Instance type, AMI, security group rules, Neo4j install steps, how to connect |
| `export_edges.sql` or `.py` | Builds `bq1_graph_edges` from `transactions_clean` |
| `import.sh` and `import.cypher` | Loads the edges into Neo4j (this is what Step Functions will run via SSM) |
| `queries.cypher` | Multi-hop paths, betweenness (intermediary banks), Louvain (clusters) |
| `results.md` | Key findings and Bloom screenshots |

## Notes for this role

- Security group: SSH and Neo4j ports (7474, 7687) from **My IP** only, never `0.0.0.0/0`.
- Attach an IAM role with `AmazonSSMManagedInstanceCore` (and S3 read) so Step Functions can trigger the import and teammates can use Session Manager instead of sharing `.pem` files.
- **Stop the instance when not in use.** It is the biggest idle cost in the project.
- Never commit the `.pem` key or Neo4j password. Put the password in a local `.env` (ignored by Git).
- Confirm early whether Bloom works on your setup. Fallback: Neo4j Browser.

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