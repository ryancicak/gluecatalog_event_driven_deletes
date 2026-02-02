# Event-Driven Lambda (Delete Files)

This uses EventBridge S3 events to detect new Iceberg delete files and trigger compaction.

## Lambda env vars
- STEP_FUNCTION_ARN (required)
- DELETE_SUFFIX (optional, default: -deletes.parquet)
- LOCK_TABLE (optional, default: iceberg_delete_locks)
- QUEUE_URL (optional, created by deploy script)
- RETRY_DELAY_SECONDS (optional, default: 300)
- LOCK_TTL_SECONDS (optional, default: 300)
- TABLE_ALLOWLIST (optional, comma-separated table_id prefixes)

## Flow
S3 ObjectCreated -> EventBridge rule (suffix filter) -> Lambda -> Step Functions

## Notes
- This does not scan S3. It reacts to new delete files.
- If you want to guard against false positives, Lambda can additionally read Iceberg metadata and confirm.
- A per-table lock is used to avoid overlapping compactions on the same table. Different tables can run in parallel.
- If TABLE_ALLOWLIST is set, only those table prefixes trigger compaction.
