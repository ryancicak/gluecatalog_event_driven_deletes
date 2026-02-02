import json
import os
import time
import boto3

STEP_FUNCTION_ARN = os.environ.get("STEP_FUNCTION_ARN")
DELETE_SUFFIX = os.environ.get("DELETE_SUFFIX", "-deletes.parquet")
LOCK_TABLE = os.environ.get("LOCK_TABLE", "iceberg_delete_locks")
QUEUE_URL = os.environ.get("QUEUE_URL")
RETRY_DELAY_SECONDS = int(os.environ.get("RETRY_DELAY_SECONDS", "300"))
LOCK_TTL_SECONDS = int(os.environ.get("LOCK_TTL_SECONDS", "900"))
CATALOG_NAME = os.environ.get("CATALOG_NAME", "glue_catalog")
WAREHOUSE_S3 = os.environ.get("WAREHOUSE_S3", "")

# TABLE_MAPPINGS format: "s3_prefix:db.table,s3_prefix2:db2.table2"
# Example: "federation_demo_db_ryan/customers_iceberg:federation_demo_db_ryan.customers_iceberg"
# If not set, auto-derives from S3 path (assumes path matches db/table structure)
TABLE_MAPPINGS = {}
for mapping in os.environ.get("TABLE_MAPPINGS", "").split(","):
    mapping = mapping.strip()
    if ":" in mapping:
        s3_prefix, glue_table = mapping.split(":", 1)
        TABLE_MAPPINGS[s3_prefix.strip()] = glue_table.strip()

ALLOWLIST = {
    item.strip()
    for item in os.environ.get("TABLE_ALLOWLIST", "").split(",")
    if item.strip()
}

sf = boto3.client("stepfunctions")
ddb = boto3.client("dynamodb")
sqs = boto3.client("sqs")


def _is_delete_file(key: str) -> bool:
    return key.endswith(DELETE_SUFFIX)


def _table_id_from_key(key: str) -> tuple:
    """
    Extract table info from S3 key.
    Returns (s3_table_prefix, glue_table_identifier)
    
    Example:
      key = "federation_demo_db_ryan/customers_iceberg/data/00001-deletes.parquet"
      returns ("federation_demo_db_ryan/customers_iceberg", "glue_catalog.federation_demo_db_ryan.customers_iceberg")
    """
    # Extract S3 prefix (everything before /data/ or /metadata/)
    s3_prefix = key
    for marker in ("/data/", "/metadata/"):
        if marker in key:
            s3_prefix = key.split(marker)[0]
            break
    else:
        s3_prefix = key.rsplit("/", 1)[0]
    
    # Look up in explicit mappings first
    if s3_prefix in TABLE_MAPPINGS:
        glue_table = f"{CATALOG_NAME}.{TABLE_MAPPINGS[s3_prefix]}"
    else:
        # Auto-derive: assume S3 path is database/table, convert to catalog.database.table
        # e.g., "federation_demo_db_ryan/customers_iceberg" -> "glue_catalog.federation_demo_db_ryan.customers_iceberg"
        glue_table = f"{CATALOG_NAME}.{s3_prefix.replace('/', '.')}"
    
    return (s3_prefix, glue_table)


def _acquire_lock(table_id: str) -> bool:
    now = int(time.time())
    lock_until = now + LOCK_TTL_SECONDS
    try:
        ddb.put_item(
            TableName=LOCK_TABLE,
            Item={
                "table_id": {"S": table_id},
                "lock_until": {"N": str(lock_until)},
            },
            ConditionExpression="attribute_not_exists(table_id) OR lock_until < :now",
            ExpressionAttributeValues={":now": {"N": str(now)}},
        )
        return True
    except ddb.exceptions.ConditionalCheckFailedException:
        return False


def _is_retry_already_queued(table_id: str) -> bool:
    """Check if a retry is already queued for this table (stored in DynamoDB)."""
    now = int(time.time())
    try:
        resp = ddb.get_item(
            TableName=LOCK_TABLE,
            Key={"table_id": {"S": table_id}},
            ProjectionExpression="retry_queued_until",
        )
        item = resp.get("Item", {})
        retry_until = int(item.get("retry_queued_until", {}).get("N", "0"))
        return retry_until > now
    except Exception:
        return False


def _mark_retry_queued(table_id: str) -> bool:
    """Mark that a retry is queued. Returns True if successfully marked (no existing retry)."""
    now = int(time.time())
    retry_until = now + RETRY_DELAY_SECONDS + 60  # Add buffer
    try:
        ddb.update_item(
            TableName=LOCK_TABLE,
            Key={"table_id": {"S": table_id}},
            UpdateExpression="SET retry_queued_until = :until",
            ConditionExpression="attribute_not_exists(retry_queued_until) OR retry_queued_until < :now",
            ExpressionAttributeValues={
                ":until": {"N": str(retry_until)},
                ":now": {"N": str(now)},
            },
        )
        return True
    except ddb.exceptions.ConditionalCheckFailedException:
        return False  # Retry already queued


def _clear_retry_marker(table_id: str) -> None:
    """Clear the retry marker after processing."""
    try:
        ddb.update_item(
            TableName=LOCK_TABLE,
            Key={"table_id": {"S": table_id}},
            UpdateExpression="REMOVE retry_queued_until",
        )
    except Exception:
        pass


def _enqueue_retry_for_table(s3_prefix: str, glue_table: str, bucket: str) -> bool:
    """Queue ONE retry per table. Returns True if queued, False if already queued."""
    if not QUEUE_URL:
        return False
    
    # Check/set marker in DynamoDB to ensure only 1 retry per table
    if not _mark_retry_queued(s3_prefix):
        return False  # Retry already queued, skip
    
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps({
            "s3_prefix": s3_prefix,
            "glue_table": glue_table,
            "bucket": bucket
        }),
        DelaySeconds=min(RETRY_DELAY_SECONDS, 900),  # SQS max is 900
    )
    return True


def _extract_events(event):
    """
    Parse incoming events from EventBridge, S3 notifications, or SQS retries.
    Returns list of tuples: (bucket, s3_key_or_none, s3_prefix_or_none, glue_table_or_none, is_retry)
    """
    records = event.get("Records")
    if records is None and "detail" in event:
        records = [event]

    events = []
    for record in records or []:
        # SQS retry message
        if record.get("eventSource") == "aws:sqs":
            body = json.loads(record["body"])
            if "glue_table" in body:
                # New format with glue_table
                events.append((body["bucket"], None, body["s3_prefix"], body["glue_table"], True))
            elif "table_id" in body:
                # Old format (backwards compatibility)
                events.append((body["bucket"], None, body["table_id"], None, True))
            else:
                events.append((body["bucket"], body["key"], None, None, False))
            continue

        # EventBridge detail
        detail = record.get("detail", {})
        if "object" in detail and "bucket" in detail:
            events.append((detail["bucket"]["name"], detail["object"]["key"], None, None, False))
            continue

        # S3 notification
        s3 = record.get("s3")
        if s3:
            events.append((s3["bucket"]["name"], s3["object"]["key"], None, None, False))

    return events


def handler(event, context):
    # Deduplicate by s3_prefix - only trigger ONE compaction per table
    # tables_to_compact: {s3_prefix: (glue_table, bucket)}
    tables_to_compact = {}
    is_retry = False

    for bucket, s3_key, s3_prefix, glue_table, is_retry_msg in _extract_events(event):
        if is_retry_msg:
            is_retry = True
            if glue_table:
                # New retry format with glue_table
                _clear_retry_marker(s3_prefix)
            else:
                # Old format - derive glue_table from s3_prefix
                s3_prefix, glue_table = _table_id_from_key(s3_prefix + "/data/dummy")
                _clear_retry_marker(s3_prefix)
        else:
            # This is a delete file event from S3
            if not s3_key or not _is_delete_file(s3_key):
                continue
            s3_prefix, glue_table = _table_id_from_key(s3_key)
        
        if ALLOWLIST and s3_prefix not in ALLOWLIST:
            continue
        
        # Only process each table once per invocation
        if s3_prefix not in tables_to_compact:
            tables_to_compact[s3_prefix] = (glue_table, bucket)

    if not tables_to_compact:
        return {"status": "no-op", "matched": 0}

    if not STEP_FUNCTION_ARN:
        raise RuntimeError("STEP_FUNCTION_ARN is not set")

    triggered = 0
    retried = 0
    skipped = 0

    for s3_prefix, (glue_table, bucket) in tables_to_compact.items():
        if not _acquire_lock(s3_prefix):
            if _enqueue_retry_for_table(s3_prefix, glue_table, bucket):
                retried += 1
            else:
                skipped += 1  # Retry already queued, skip
            continue

        # Pass glue_table and warehouse to Step Function for dynamic compaction
        sf.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            input=json.dumps({
                "table_id": s3_prefix,
                "glue_table": glue_table,
                "warehouse_s3": WAREHOUSE_S3 or f"s3://{bucket}/",
            }),
        )
        triggered += 1

    return {
        "status": "triggered" if triggered > 0 else ("queued" if retried > 0 else "skipped"),
        "tables": list(tables_to_compact.keys()),
        "started": triggered,
        "retried": retried,
        "skipped": skipped,
        "is_retry": is_retry
    }
