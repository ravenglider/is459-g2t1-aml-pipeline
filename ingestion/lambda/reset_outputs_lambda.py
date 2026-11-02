import json

import boto3

ALLOWED_BUCKET = "is459-g2t1-aml-processed-bucket"

s3 = boto3.client("s3")
glue = boto3.client("glue")


def delete_table(database, table):
    try:
        glue.delete_table(DatabaseName=database, Name=table)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


def clear_prefix(bucket, prefix):
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        # delete_objects accepts up to 1000 keys per call; one page is at most 1000
        if keys:
            resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            errors = resp.get("Errors", [])
            if errors:
                raise RuntimeError(f"Failed to delete some objects under {prefix}: {errors[:3]}")
            deleted += len(keys)
    return deleted


def lambda_handler(event, context):
    bucket = event["bucket"]
    database = event["database"]
    outputs = event["outputs"]

    if bucket != ALLOWED_BUCKET:
        raise ValueError(f"Refusing to clear bucket {bucket}: only {ALLOWED_BUCKET} is allowed")

    summary = []
    for out in outputs:
        table, prefix = out["table"], out["prefix"]
        if not prefix or not prefix.endswith("/") or prefix.strip("/") == "":
            raise ValueError(f"Unsafe prefix {prefix!r}: must be a folder name ending in '/'")

        table_dropped = delete_table(database, table)
        objects_deleted = clear_prefix(bucket, prefix)
        summary.append(
            {"table": table, "table_dropped": table_dropped, "prefix": prefix, "objects_deleted": objects_deleted}
        )

    print(json.dumps(summary))
    return {"bucket": bucket, "database": database, "reset": summary}