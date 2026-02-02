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


def _table_id_from_key(key: str) -> str:
    # Expect .../data/... or .../metadata/...; use prefix as table id.
    for marker in ("/data/", "/metadata/"):
        if marker in key:
            return key.split(marker)[0]
    return key.rsplit("/", 1)[0]


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


def _enqueue_retry_for_table(table_id: str, bucket: str) -> bool:
    """Queue ONE retry per table. Returns True if queued, False if already queued."""
    if not QUEUE_URL:
        return False
    
    # Check/set marker in DynamoDB to ensure only 1 retry per table
    if not _mark_retry_queued(table_id):
        return False  # Retry already queued, skip
    
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps({"table_id": table_id, "bucket": bucket}),
        DelaySeconds=min(RETRY_DELAY_SECONDS, 900),  # SQS max is 900
    )
    return True


def _extract_events(event):
    # Supports EventBridge S3 events, S3 notifications, or SQS messages
    records = event.get("Records")
    if records is None and "detail" in event:
        records = [event]

    events = []
    for record in records or []:
        # SQS
        if record.get("eventSource") == "aws:sqs":
            body = json.loads(record["body"])
            # Handle both old format (bucket, key) and new format (table_id, bucket)
            if "table_id" in body:
                events.append((body["bucket"], body["table_id"], True))
            else:
                events.append((body["bucket"], body["key"], False))
            continue

        # EventBridge detail
        detail = record.get("detail", {})
        if "object" in detail and "bucket" in detail:
            events.append((detail["bucket"]["name"], detail["object"]["key"], False))
            continue

        # S3 notification
        s3 = record.get("s3")
        if s3:
            events.append((s3["bucket"]["name"], s3["object"]["key"], False))

    return events


def handler(event, context):
    # Deduplicate by table_id - only trigger ONE compaction per table
    tables_to_compact = {}
    is_retry = False

    for bucket, key_or_table_id, is_table_id in _extract_events(event):
        if is_table_id:
            # This is a retry message with table_id directly
            table_id = key_or_table_id
            is_retry = True
            # Clear the retry marker since we're processing it now
            _clear_retry_marker(table_id)
        else:
            # This is a delete file event
            if not _is_delete_file(key_or_table_id):
                continue
            table_id = _table_id_from_key(key_or_table_id)
        
        if ALLOWLIST and table_id not in ALLOWLIST:
            continue
        
        # Only process each table once per invocation
        if table_id not in tables_to_compact:
            tables_to_compact[table_id] = bucket

    if not tables_to_compact:
        return {"status": "no-op", "matched": 0}

    if not STEP_FUNCTION_ARN:
        raise RuntimeError("STEP_FUNCTION_ARN is not set")

    triggered = 0
    retried = 0
    skipped = 0

    for table_id, bucket in tables_to_compact.items():
        if not _acquire_lock(table_id):
            if _enqueue_retry_for_table(table_id, bucket):
                retried += 1
            else:
                skipped += 1  # Retry already queued, skip
            continue

        sf.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            input=json.dumps({"table_id": table_id}),
        )
        triggered += 1

    return {
        "status": "triggered" if triggered > 0 else ("queued" if retried > 0 else "skipped"),
        "tables": len(tables_to_compact),
        "started": triggered,
        "retried": retried,
        "skipped": skipped,
        "is_retry": is_retry
    }
