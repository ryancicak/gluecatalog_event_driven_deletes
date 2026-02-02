# Glue Catalog Event-Driven Deletes

**Important:** Databricks recommends upgrading to Iceberg V3, which natively supports row-level deletes. If you can upgrade, do that instead of using this solution.

However, if you are unable to upgrade to V3 in the short-term and you rely on Merge-on-Read (MoR) for other engines like Trino, this event-driven solution can help.

## The Problem

Databricks Unity Catalog cannot read Iceberg tables that have MoR delete files. This pipeline automatically compacts those delete files when they appear, so Databricks can read the table.

## How It Works

1. You run a DELETE on an Iceberg table in AWS Glue Catalog
2. Iceberg writes a position delete file to S3
3. S3 sends an event to EventBridge
4. EventBridge triggers a Lambda function
5. Lambda starts a Step Function that runs compaction on EMR
6. Delete files get merged into data files
7. Databricks can now read the table

## Compaction Process

The compaction script runs two Iceberg maintenance procedures in order:

| Step | Procedure | What it does |
|------|-----------|--------------|
| 1 | `rewriteDataFiles` | Merges position delete files INTO new data files. Creates new data files without the deleted rows. |
| 2 | `rewritePositionDeletes` | Cleans up any remaining position delete file fragments. Without this, you may need multiple `rewriteDataFiles` passes. |

This sequence guarantees delete files are fully removed from the table metadata.

## Architecture

```
S3 (delete file created)
    |
    v
EventBridge
    |
    v
Lambda (iceberg-delete-detector)
    |
    v
Step Functions (iceberg-delete-compaction)
    |
    v
EMR Step (spark-shell compaction.scala)
```

## Prerequisites

- AWS account with EMR cluster running
- Iceberg table in AWS Glue Catalog
- SSH key for EMR master node
- AWS CLI configured

## Quick Start

### 1. Run setup.sh with MODE=event

One command sets up everything:

```bash
# Required
export AWS_REGION=us-west-2
export EMR_CLUSTER_ID=j-XXXXXXXXXXXXX
export EMR_KEY_PATH=/path/to/your-key.pem
export MODE=event
export BUCKET_NAMES=my-bucket                # comma-separated for multiple: bucket-a,bucket-b

# Optional (defaults shown)
export CATALOG_NAME=glue_catalog             # Spark catalog name
export DELETE_SUFFIX=-deletes.parquet        # suffix to detect delete files
export LOCK_TTL_SECONDS=300                  # how long to hold lock during compaction
export RETRY_DELAY_SECONDS=300               # delay before retry if locked
export TABLE_ALLOWLIST=                      # limit to specific tables (comma-separated S3 prefixes)
export TABLE_MAPPINGS=                       # custom S3 path to Glue table mappings

# Resource naming (defaults shown)
export STATE_MACHINE_NAME=iceberg-delete-compaction
export LAMBDA_FUNCTION_NAME=iceberg-delete-detector
export EVENT_RULE_NAME=iceberg-delete-file-events
export LOCK_TABLE_NAME=iceberg_delete_locks

./scripts/setup.sh
```

This creates:
- IAM roles for Step Functions, EventBridge, and Lambda
- Step Function state machine
- Lambda function
- EventBridge rule to detect delete files
- DynamoDB lock table
- SQS retry queue
- Compaction script on EMR

### 2. Enable S3 EventBridge notifications on each bucket

```bash
aws s3api put-bucket-notification-configuration \
  --bucket my-bucket \
  --notification-configuration '{"EventBridgeConfiguration": {}}'
```

That's it! The pipeline will now automatically compact tables when delete files appear.

## Multiple Tables

This pipeline supports multiple tables automatically. When a delete file is created in S3, the Lambda:

1. Detects which bucket the delete file is in
2. Extracts the S3 path (e.g., `mydb/mytable/data/...`)
3. Converts it to a Glue table identifier (e.g., `glue_catalog.mydb.mytable`)
4. Passes the table and warehouse location to the Step Function
5. Compacts only that specific table

Each table has its own lock, so compactions for different tables can run in parallel.

Tables can be in different S3 buckets. The pipeline automatically uses the bucket from the S3 event.

### Custom Table Mappings

If your S3 paths don't match your Glue database/table names, use `TABLE_MAPPINGS`.

The format is `s3_path:glue_db.glue_table` where `s3_path` is the path within the bucket (not including `s3://` or bucket name):

```bash
# Example: table is at s3://mybucket/warehouse/sales/orders/data/...
# but Glue table is production_db.orders_table
export TABLE_MAPPINGS="warehouse/sales/orders:production_db.orders_table"
./scripts/deploy_lambda_eventbridge.sh
```

Multiple mappings separated by commas:
```bash
export TABLE_MAPPINGS="path1/table1:db1.table1,path2/table2:db2.table2"
```

### Allowlist Specific Tables

To only trigger compaction for specific tables:

```bash
export TABLE_ALLOWLIST="mydb/table1,mydb/table2"
./scripts/deploy_lambda_eventbridge.sh
```

## Testing

Run a delete on your table:

```sql
DELETE FROM glue_catalog.your_database.your_table WHERE id = 1;
```

Watch the compaction run:

```bash
aws emr list-steps --cluster-id $EMR_CLUSTER_ID --query 'Steps[0]'
```

## How the Lock Works

The Lambda uses DynamoDB to prevent concurrent compaction runs on the same table. If a compaction is already running, new delete events queue a retry via SQS. Only one retry per table is queued at a time.

## Files

| File | Purpose |
|------|---------|
| `scripts/setup.sh` | Main setup script |
| `scripts/deploy_lambda_eventbridge.sh` | Deploys Lambda and EventBridge rule |
| `scripts/lambda_detect_delete.py` | Lambda that triggers compaction |
| `scripts/compaction.scala` | Spark script that runs compaction |
| `scripts/state_machine.json` | Step Function definition |

## Scaling

The default config uses YARN with dynamic allocation (up to 20 executors). For very large tables, you may need to:

- Increase EMR cluster size
- Adjust `--num-executors` and `--executor-memory` in setup.sh
- Increase the Step Function timeout

See `docs/SCALING.md` for details.

## Troubleshooting

**Compaction not triggering?**
- Check S3 EventBridge is enabled on your bucket
- Check Lambda logs in CloudWatch
- Check DynamoDB lock table for stuck locks

**Delete files not being removed?**
- The compaction uses `rewrite-all=true` which should handle all cases
- Check EMR step logs: `/var/log/hadoop/steps/STEP_ID/stdout`

**Databricks still can't read?**
- Verify compaction completed successfully
- Check that no new delete files were written after compaction
