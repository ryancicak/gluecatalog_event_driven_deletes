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

### 1. Set environment variables

```bash
export AWS_REGION=us-west-2
export EMR_CLUSTER_ID=j-XXXXXXXXXXXXX
export EMR_KEY_PATH=/path/to/your-key.pem
export TABLE_IDENT=glue_catalog.your_database.your_table
export WAREHOUSE_S3=s3://your-bucket/
```

### 2. Run the setup script

```bash
./scripts/setup.sh
```

This creates:
- IAM roles for Step Functions and EventBridge
- Step Function state machine
- Compaction script on EMR

### 3. Deploy the Lambda and EventBridge rule

```bash
export STEP_FUNCTION_ARN=arn:aws:states:us-west-2:ACCOUNT:stateMachine:iceberg-delete-compaction
export BUCKET_NAME=your-bucket

./scripts/deploy_lambda_eventbridge.sh
```

### 4. Enable S3 EventBridge notifications

```bash
aws s3api put-bucket-notification-configuration \
  --bucket $BUCKET_NAME \
  --notification-configuration '{"EventBridgeConfiguration": {}}'
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
