# Role 4 EC2 and Neo4j setup record

This records the shared server configuration used for the validated manual sample and HI runs. Existing installation is complete; do not recreate containers or credentials merely to read these notes. The personal step-by-step walkthrough stays local and is excluded from GitHub; this setup record accompanies the project source.

## Existing resources

| Item | Value |
|---|---|
| Region | `us-east-1` |
| Instance | `is459-g2t1-ingest`, `i-0125056b848e5634a` |
| OS | Amazon Linux 2023, x86_64 |
| Instance size | Temporarily resized from `t3.small` to `t3.medium` with team agreement |
| Observed RAM / disk | About 3.7 GiB RAM after resize; 50 GiB root disk |
| Instance role | `is459-g2t1-ec2-s3-role` |
| Manually verified policies | `AmazonS3FullAccess`, `AmazonSSMManagedInstanceCore` |
| Login | AWS Systems Manager Session Manager, then `bash` |
| Docker | Client 25.0.14 / Engine 25.0.18 at setup |
| Image / observed database | `neo4j:5.26-community` / Neo4j 5.26.32 Community |
| GDS | `graph-data-science` plugin; verify live with `RETURN gds.version()` |
| Container | `role4-neo4j` |
| Persistent Docker volumes | `role4-data`, `role4-logs`, `role4-plugins` |
| Host work directory | `/home/ssm-user/role4/` |
| Local credentials | Protected `.env` in the host work directory; never copied into Git |

The attached S3/SSM policies do not establish Athena/Glue access for the new Python runner. Those permissions still need verification before an automated trial. The raw bucket stays read-only.

## First-time preparation, only if rebuilding the approved setup

The existing server is already prepared. On a fresh agreed Amazon Linux 2023 setup, check OS/RAM/disk and the attached role first, then prepare Docker and directories:

```bash
sudo dnf install -y docker python3
sudo systemctl enable --now docker
mkdir -p "$HOME/role4/import" "$HOME/role4/results"
sudo docker volume create role4-data
sudo docker volume create role4-logs
sudo docker volume create role4-plugins
```

AWS CLI v2 was already present on the observed server. Verify with `aws --version`; use the [official installation instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) if rebuilding without it. Do not configure personal AWS keys; use the instance role.

For a genuinely new database only, create the credential file interactively without printing the password. This preserves an existing file and does not reset the password of an existing database volume:

```bash
if [ ! -e "$HOME/role4/.env" ]; then
  (
    umask 077
    read -r -s -p 'Initial Neo4j password: ' role4_password
    printf '\n'
    if [ "${#role4_password}" -lt 8 ]; then
      printf 'Use a password of at least 8 characters.\n' >&2
      exit 1
    fi
    printf 'NEO4J_AUTH=neo4j/%s\n' "$role4_password" > "$HOME/role4/.env"
    unset role4_password
  )
else
  printf 'Existing credential file preserved.\n'
fi
```

Do not proceed to the container launch if preparation fails. Never capture the password or `.env` contents in screenshots, logs or video.

## Container configuration used

This is the launch configuration for reproduction or recovery, **not a command to run against an existing named container**. Initial preparation is recorded above. Never remove persistent volumes as part of a container-only recovery.

```bash
sudo docker run -d \
  --name role4-neo4j \
  --restart unless-stopped \
  --memory=2560m --memory-swap=2560m \
  -p 7474:7474 -p 7687:7687 \
  --env-file "$HOME/role4/.env" \
  -e 'NEO4J_PLUGINS=["graph-data-science"]' \
  -e 'NEO4J_dbms_security_procedures_unrestricted=gds.*' \
  -e NEO4J_server_memory_heap_initial__size=512M \
  -e NEO4J_server_memory_heap_max__size=1G \
  -e NEO4J_server_memory_pagecache_size=512M \
  -v role4-data:/data \
  -v role4-logs:/logs \
  -v role4-plugins:/plugins \
  -v "$HOME/role4/import:/var/lib/neo4j/import:rw" \
  neo4j:5.26-community
```

The import bind mount must be writable at container startup because this image changes its ownership. A read-only mount caused a restart loop during setup; that was corrected. File ownership can become UID/GID 7474, so use `sudo` for host downloads/header checks and verify readability as the container user.

HI files are under `import/hi-01/`; their Neo4j URLs are `file:///hi-01/bank_edges.csv` and `file:///hi-01/transactions.csv`. Sample files were kept separately. The database has scope-qualified Bank and Account uniqueness constraints; never mix a new CSV population into an accepted scope.

## Connection and verification

Security-group inbound TCP 7474 and 7687 are restricted to the user's current public IPv4 `/32` (console **My IP**), not the EC2 instance's IP. Preserve other team rules. Use Session Manager for terminal access. Browser connects to `http://EC2_PUBLIC_IP:7474` and Bolt on port 7687; the public IP can change after stopping/starting EC2.

In Session Manager:

```bash
bash
sudo docker ps -a
sudo docker logs --tail 50 role4-neo4j
sudo docker exec --user 7474 role4-neo4j head -n 1 /var/lib/neo4j/import/hi-01/bank_edges.csv
sudo docker exec --user 7474 role4-neo4j head -n 1 /var/lib/neo4j/import/hi-01/transactions.csv
```

In Neo4j Browser, run separately:

```cypher
CALL dbms.components();

RETURN gds.version() AS gds_version;

SHOW PROCEDURES YIELD name
WHERE name IN ['gds.graph.project','gds.betweenness.write','gds.louvain.write']
RETURN name;
```

The configured heap is 1 GiB and container cap 2.5 GiB; host free RAM is not all usable GDS heap. Inspect graph/algorithm estimates before running. The Python runner additionally limits algorithm estimates to a configurable budget but does not replace live capacity checks.

On 3 November 2026 the team approved Neo4j Browser instead of Bloom for the final visuals, as reported by the user. Use the existing Browser connection; no Bloom installation/licence is needed for the agreed route. The three final scenes/captions remain pending. Record the proposal-tool change in the report/slides.

## Evidence and stopping

- [Running container](screenshots/02k-neo4j-container-started.png)
- [Database version](screenshots/02m-neo4j-version.png)
- [GDS version](screenshots/02n-gds-version.png)
- [Required GDS procedures](screenshots/02o-gds-required-procedures.png)

Save accepted results first. Coordinate with the group, end the Session Manager session and stop the EC2 instance when idle; do not terminate it during development. Stopping a browser tab or Docker alone does not stop EC2 charges. Final resource deletion is coordinated after the presentation.
