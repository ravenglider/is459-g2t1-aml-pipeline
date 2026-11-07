# Connecting Tableau to Athena

## Driver

Tableau Desktop needs the Amazon Athena JDBC driver installed locally (not ODBC, despite what you might expect).

1. Download the latest `.jar` from the [Athena JDBC 2.x driver page](https://docs.aws.amazon.com/athena/latest/ug/connect-with-jdbc.html).
2. Copy it into `~/Library/Tableau/Drivers` on Mac (create the folder if it doesn't exist).
3. Restart Tableau Desktop.

## Connection settings

| Field | Value |
|---|---|
| Server | `athena.us-east-1.amazonaws.com` |
| Port | `443` |
| S3 Staging Directory | `s3://is459-g2t1-aml-athena-bucket/results/` |
| Authentication | Username and Password |
| Access Key ID | *(your own AWS Access Key ID)* |
| Secret Access Key | *(your own AWS Secret Access Key — never commit this)* |
| Catalog | AwsDataCatalog |
| Database | is459_g2t1_aml |

Use your own IAM credentials for the `g2team1` account. Do not reuse or share Access Key IDs/Secret Access Keys in chat, commits, or this file.

Note: do not append `;Workgroup=primary` to the Server field — it breaks the JDBC driver's region parsing. The account's default workgroup is already `primary`, so leaving it off works correctly.
