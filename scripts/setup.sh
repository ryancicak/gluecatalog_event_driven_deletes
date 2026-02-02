#!/usr/bin/env bash
set -euo pipefail

# Required env vars
: "${AWS_REGION:?Set AWS_REGION}"
: "${EMR_CLUSTER_ID:?Set EMR_CLUSTER_ID}"
: "${EMR_MASTER_DNS:=}"
: "${EMR_KEY_PATH:?Set EMR_KEY_PATH (path to pem)}"

# Optional - only needed for scheduled mode (event mode gets these dynamically)
TABLE_IDENT="${TABLE_IDENT:-}"
WAREHOUSE_S3="${WAREHOUSE_S3:-}"

# Mode: scheduled (runs on a schedule for a single table) or event (triggered by S3 events, multi-table)
MODE="${MODE:-scheduled}" # scheduled | event
SCHEDULE_EXPRESSION="${SCHEDULE_EXPRESSION:-rate(1 hour)}"
STATE_MACHINE_NAME="${STATE_MACHINE_NAME:-iceberg-delete-compaction}"
EVENTBRIDGE_RULE_NAME="${EVENTBRIDGE_RULE_NAME:-iceberg-delete-compaction-schedule}"
STATE_MACHINE_ROLE_NAME="${STATE_MACHINE_ROLE_NAME:-iceberg-delete-compaction-sfn-role}"
EVENTBRIDGE_ROLE_NAME="${EVENTBRIDGE_ROLE_NAME:-iceberg-delete-compaction-events-role}"

WORKDIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$WORKDIR/.." && pwd)"
TMPDIR="${PROJECT_ROOT}/.tmp"
mkdir -p "$TMPDIR"

# 1) Generate compaction.scala - reads table from TABLE_IDENT env var
cat > "$TMPDIR/compaction.scala" <<'SCALA'
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.spark.Spark3Util

// Read table identifier from environment variable (set by Step Function)
val tableIdent = sys.env.getOrElse("TABLE_IDENT", throw new RuntimeException("TABLE_IDENT env var not set"))
println(s"Compacting table: ${tableIdent}")
val table = Spark3Util.loadIcebergTable(spark, tableIdent)

def deleteFileCount(): Long = {
  spark
    .sql(s"SELECT COUNT(1) AS c FROM ${tableIdent}.delete_files")
    .collect()(0)
    .getLong(0)
}

val maxPasses = sys.env.getOrElse("COMPACTION_PASSES", "5").toInt
var pass = 0
var lastCount = deleteFileCount()
println(s"Delete files before compaction: ${lastCount}")

while (pass < maxPasses && lastCount > 0) {
  pass += 1
  
  // rewrite-all=true ensures ALL delete files get cleaned up in one pass
  val dataResult = SparkActions.get(spark)
    .rewriteDataFiles(table)
    .option("rewrite-all", "true")
    .option("delete-file-threshold", "1")
    .option("target-file-size-bytes", "536870912")  // 512MB target files
    .execute()

  println(
    s"Pass ${pass} rewriteDataFiles: addedDataFiles=${dataResult.addedDataFilesCount()}, " +
      s"rewrittenDataFiles=${dataResult.rewrittenDataFilesCount()}, " +
      s"rewrittenBytes=${dataResult.rewrittenBytesCount()}, " +
      s"removedDeleteFiles=${dataResult.removedDeleteFilesCount()}, " +
      s"failedDataFiles=${dataResult.failedDataFilesCount()}"
  )

  // Then, compact any remaining position delete files
  try {
    val deleteResult = SparkActions.get(spark)
      .rewritePositionDeletes(table)
      .execute()
    println(
      s"Pass ${pass} rewritePositionDeletes: rewrittenFiles=${deleteResult.rewrittenDeleteFilesCount()}, " +
        s"addedFiles=${deleteResult.addedDeleteFilesCount()}"
    )
  } catch {
    case e: Exception => println(s"rewritePositionDeletes skipped: ${e.getMessage}")
  }

  // Refresh table to get latest metadata
  table.refresh()

  val newCount = deleteFileCount()
  println(s"Delete files after pass ${pass}: ${newCount}")
  if (newCount >= lastCount && pass > 1) {
    println("Delete file count did not decrease after multiple passes; stopping.")
    lastCount = newCount
    pass = maxPasses
  } else {
    lastCount = newCount
  }
}

println(s"Final delete file count: ${lastCount}")

// Expire old snapshots to clean up references to old delete files
try {
  val expireResult = SparkActions.get(spark)
    .expireSnapshots(table)
    .retainLast(1)
    .execute()
  println(s"Expired snapshots: deleted=${expireResult.deletedDataFilesCount()} data files, " +
    s"${expireResult.deletedDeleteFilesCount()} delete files, " +
    s"${expireResult.deletedManifestsCount()} manifests")
} catch {
  case e: Exception => println(s"expireSnapshots skipped: ${e.getMessage}")
}

spark.stop()
System.exit(0)
SCALA

# 2) Upload compaction.scala to EMR master (resolve DNS if not provided)
if [ -z "$EMR_MASTER_DNS" ]; then
  EMR_MASTER_DNS=$(aws emr describe-cluster \
    --region "$AWS_REGION" \
    --cluster-id "$EMR_CLUSTER_ID" \
    --query 'Cluster.MasterPublicDnsName' \
    --output text)
fi

if [ -z "$EMR_MASTER_DNS" ] || [ "$EMR_MASTER_DNS" = "None" ]; then
  echo "Unable to resolve EMR master DNS. Set EMR_MASTER_DNS or ensure the cluster is public."
  exit 1
fi

scp -i "$EMR_KEY_PATH" -o StrictHostKeyChecking=no "$TMPDIR/compaction.scala" "hadoop@${EMR_MASTER_DNS}:/home/hadoop/compaction.scala"

# 3) Build EMR step JSON (uses YARN for horizontal scaling)
python3 - <<PY > "$TMPDIR/emr_step.json"
import json
step = {
  "Name": "iceberg_delete_compaction",
  "ActionOnFailure": "CONTINUE",
  "HadoopJarStep": {
    "Jar": "command-runner.jar",
    "Args": [
      "bash", "-lc",
      "timeout 60m bash -lc 'TABLE_IDENT=${TABLE_IDENT} spark-shell \
        --master yarn \
        --deploy-mode client \
        --num-executors 4 \
        --executor-cores 2 \
        --executor-memory 4g \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
        --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
        --conf spark.sql.catalog.glue_catalog.warehouse=${WAREHOUSE_S3} \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
        --conf spark.dynamicAllocation.enabled=true \
        --conf spark.dynamicAllocation.minExecutors=1 \
        --conf spark.dynamicAllocation.maxExecutors=20 \
        -i /home/hadoop/compaction.scala' <<< ':quit'"
    ]
  }
}
print(json.dumps(step))
PY

# 4) Create IAM role for Step Functions if needed
SFN_ROLE_ARN=$(aws iam get-role --role-name "$STATE_MACHINE_ROLE_NAME" --query Role.Arn --output text 2>/dev/null || true)
if [ -z "$SFN_ROLE_ARN" ] || [ "$SFN_ROLE_ARN" = "None" ]; then
  cat > "$TMPDIR/sfn_trust.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "states.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON
  SFN_ROLE_ARN=$(aws iam create-role \
    --role-name "$STATE_MACHINE_ROLE_NAME" \
    --assume-role-policy-document file://"$TMPDIR/sfn_trust.json" \
    --query Role.Arn --output text)
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
cat > "$TMPDIR/sfn_policy.json" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "elasticmapreduce:AddJobFlowSteps",
        "elasticmapreduce:DescribeCluster",
        "elasticmapreduce:DescribeStep",
        "elasticmapreduce:CancelSteps",
        "elasticmapreduce:ListSteps"
      ],
      "Resource": ["*"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "events:PutTargets",
        "events:PutRule",
        "events:DescribeRule"
      ],
      "Resource": ["arn:aws:events:${AWS_REGION}:${ACCOUNT_ID}:rule/StepFunctionsGetEventForEMRAddJobFlowStepsRule"]
    }
  ]
}
JSON

aws iam put-role-policy \
  --role-name "$STATE_MACHINE_ROLE_NAME" \
  --policy-name iceberg-delete-compaction-sfn \
  --policy-document file://"$TMPDIR/sfn_policy.json"

# 5) Create Step Functions state machine with dynamic table support
# The state machine receives {glue_table, warehouse_s3, table_id} from Lambda
# and dynamically constructs the EMR step command
cat > "$TMPDIR/state_machine.json" <<JSON
{
  "Comment": "Run Iceberg delete compaction on EMR - supports multiple tables",
  "StartAt": "AddEmrStep",
  "States": {
    "AddEmrStep": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:addStep.sync",
      "Parameters": {
        "ClusterId": "${EMR_CLUSTER_ID}",
        "Step": {
          "Name": "iceberg_delete_compaction",
          "ActionOnFailure": "CONTINUE",
          "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args.$": "States.Array('bash', '-lc', States.Format('timeout 60m bash -lc \\'TABLE_IDENT={} WAREHOUSE_S3={} spark-shell --master yarn --deploy-mode client --num-executors 4 --executor-cores 2 --executor-memory 4g --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.warehouse={} --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 -i /home/hadoop/compaction.scala\\' <<< \\':quit\\'', \$.glue_table, \$.warehouse_s3, \$.warehouse_s3))"
          }
        }
      },
      "End": true
    }
  }
}
JSON

STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines \
  --region "$AWS_REGION" \
  --query "stateMachines[?name=='${STATE_MACHINE_NAME}'].stateMachineArn | [0]" \
  --output text)

if [ "$STATE_MACHINE_ARN" = "None" ] || [ -z "$STATE_MACHINE_ARN" ]; then
  STATE_MACHINE_ARN=$(aws stepfunctions create-state-machine \
    --region "$AWS_REGION" \
    --name "$STATE_MACHINE_NAME" \
    --role-arn "$SFN_ROLE_ARN" \
    --definition file://"$TMPDIR/state_machine.json" \
    --query stateMachineArn \
    --output text)
else
  aws stepfunctions update-state-machine \
    --region "$AWS_REGION" \
    --state-machine-arn "$STATE_MACHINE_ARN" \
    --definition file://"$TMPDIR/state_machine.json" >/dev/null
fi

# 6) Create IAM role for EventBridge if needed
EVENT_ROLE_ARN=$(aws iam get-role --role-name "$EVENTBRIDGE_ROLE_NAME" --query Role.Arn --output text 2>/dev/null || true)
if [ -z "$EVENT_ROLE_ARN" ] || [ "$EVENT_ROLE_ARN" = "None" ]; then
  cat > "$TMPDIR/events_trust.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "events.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON
  EVENT_ROLE_ARN=$(aws iam create-role \
    --role-name "$EVENTBRIDGE_ROLE_NAME" \
    --assume-role-policy-document file://"$TMPDIR/events_trust.json" \
    --query Role.Arn --output text)
fi

cat > "$TMPDIR/events_policy.json" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["states:StartExecution"],
      "Resource": ["${STATE_MACHINE_ARN}"]
    }
  ]
}
JSON

aws iam put-role-policy \
  --role-name "$EVENTBRIDGE_ROLE_NAME" \
  --policy-name iceberg-delete-compaction-events \
  --policy-document file://"$TMPDIR/events_policy.json"

# 7) Wire trigger
if [ "$MODE" = "event" ]; then
  : "${BUCKET_NAMES:?Set BUCKET_NAMES for event-driven mode (comma-separated)}"
  export STEP_FUNCTION_ARN="$STATE_MACHINE_ARN"
  export AWS_REGION
  export BUCKET_NAMES
  export EVENT_RULE_NAME="${EVENT_RULE_NAME:-iceberg-delete-file-events}"
  export LAMBDA_FUNCTION_NAME="${LAMBDA_FUNCTION_NAME:-iceberg-delete-detector}"
  export LAMBDA_ROLE_NAME="${LAMBDA_ROLE_NAME:-iceberg-delete-detector-role}"
  export DELETE_SUFFIX="${DELETE_SUFFIX:--deletes.parquet}"
  "$WORKDIR/deploy_lambda_eventbridge.sh"
else
  aws events put-rule \
    --region "$AWS_REGION" \
    --name "$EVENTBRIDGE_RULE_NAME" \
    --schedule-expression "$SCHEDULE_EXPRESSION" \
    --state ENABLED >/dev/null

  python3 - <<PY > "$TMPDIR/targets.json"
import json
import pathlib
step = json.loads(pathlib.Path("$TMPDIR/emr_step.json").read_text())
input_json = json.dumps({"emrStep": step})
targets = [{
    "Id": "run-step-function",
    "Arn": "$STATE_MACHINE_ARN",
    "RoleArn": "$EVENT_ROLE_ARN",
    "Input": input_json
}]
print(json.dumps(targets))
PY

  aws events put-targets \
    --region "$AWS_REGION" \
    --rule "$EVENTBRIDGE_RULE_NAME" \
    --targets file://"$TMPDIR/targets.json" \
    >/dev/null
fi

cat <<OUT
Setup complete.
- Step Function: $STATE_MACHINE_ARN
- EMR step JSON: $TMPDIR/emr_step.json
- SFN Role: $SFN_ROLE_ARN
- Events Role: $EVENT_ROLE_ARN
OUT

if [ "$MODE" = "event" ]; then
  cat <<OUT
- Mode: event-driven (Lambda + EventBridge S3)
- Buckets: $BUCKET_NAMES
- Delete suffix: ${DELETE_SUFFIX:--deletes.parquet}
OUT
else
  cat <<OUT
- Mode: scheduled
- EventBridge rule: $EVENTBRIDGE_RULE_NAME ($SCHEDULE_EXPRESSION)
OUT
fi
