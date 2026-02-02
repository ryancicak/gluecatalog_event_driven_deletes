#!/usr/bin/env bash
set -euo pipefail

: "${AWS_REGION:?Set AWS_REGION}"
: "${STEP_FUNCTION_ARN:?Set STEP_FUNCTION_ARN}"
: "${BUCKET_NAME:?Set BUCKET_NAME (e.g., glue-federation-demo-332745928618)}"

LAMBDA_FUNCTION_NAME="${LAMBDA_FUNCTION_NAME:-iceberg-delete-detector}"
LAMBDA_ROLE_NAME="${LAMBDA_ROLE_NAME:-iceberg-delete-detector-role}"
EVENT_RULE_NAME="${EVENT_RULE_NAME:-iceberg-delete-file-events}"
DELETE_SUFFIX="${DELETE_SUFFIX:--deletes.parquet}"
LOCK_TABLE_NAME="${LOCK_TABLE_NAME:-iceberg_delete_locks}"
RETRY_DELAY_SECONDS="${RETRY_DELAY_SECONDS:-300}"
LOCK_TTL_SECONDS="${LOCK_TTL_SECONDS:-300}"
TABLE_ALLOWLIST="${TABLE_ALLOWLIST:-}"

WORKDIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$WORKDIR/.." && pwd)"
TMPDIR="${PROJECT_ROOT}/.tmp"
mkdir -p "$TMPDIR"

# 1) Create IAM role for Lambda if needed
ROLE_ARN=$(aws iam get-role --role-name "$LAMBDA_ROLE_NAME" --query Role.Arn --output text 2>/dev/null || true)
if [ -z "$ROLE_ARN" ] || [ "$ROLE_ARN" = "None" ]; then
  cat > "$TMPDIR/lambda_trust.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON
  ROLE_ARN=$(aws iam create-role \
    --role-name "$LAMBDA_ROLE_NAME" \
    --assume-role-policy-document file://"$TMPDIR/lambda_trust.json" \
    --query Role.Arn --output text)

  aws iam attach-role-policy \
    --role-name "$LAMBDA_ROLE_NAME" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
fi

# 2) Attach policy to allow Step Functions execution
cat > "$TMPDIR/lambda_sf_policy.json" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["states:StartExecution"],
      "Resource": ["${STEP_FUNCTION_ARN}"]
    }
  ]
}
JSON

aws iam put-role-policy \
  --role-name "$LAMBDA_ROLE_NAME" \
  --policy-name iceberg-delete-detector-start-sf \
  --policy-document file://"$TMPDIR/lambda_sf_policy.json"

# 2b) Create DynamoDB lock table if needed
TABLE_EXISTS=$(aws dynamodb list-tables --region "$AWS_REGION" --query "TableNames[?@=='${LOCK_TABLE_NAME}'] | [0]" --output text)
if [ "$TABLE_EXISTS" = "None" ] || [ -z "$TABLE_EXISTS" ]; then
  aws dynamodb create-table \
    --region "$AWS_REGION" \
    --table-name "$LOCK_TABLE_NAME" \
    --attribute-definitions AttributeName=table_id,AttributeType=S \
    --key-schema AttributeName=table_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null
  aws dynamodb wait table-exists \
    --region "$AWS_REGION" \
    --table-name "$LOCK_TABLE_NAME"
  aws dynamodb update-time-to-live \
    --region "$AWS_REGION" \
    --table-name "$LOCK_TABLE_NAME" \
    --time-to-live-specification "Enabled=true,AttributeName=lock_until" >/dev/null
fi

# 2c) Create SQS queue for retries if needed
QUEUE_URL=$(aws sqs get-queue-url --queue-name "${LAMBDA_FUNCTION_NAME}-retry" --query QueueUrl --output text 2>/dev/null || true)
if [ "$QUEUE_URL" = "None" ] || [ -z "$QUEUE_URL" ]; then
  QUEUE_URL=$(aws sqs create-queue \
    --queue-name "${LAMBDA_FUNCTION_NAME}-retry" \
    --attributes VisibilityTimeout=300 \
    --query QueueUrl --output text)
fi

QUEUE_ARN=$(aws sqs get-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attribute-names QueueArn \
  --query Attributes.QueueArn --output text)

ENV_VARS="STEP_FUNCTION_ARN=${STEP_FUNCTION_ARN},DELETE_SUFFIX=${DELETE_SUFFIX},LOCK_TABLE=${LOCK_TABLE_NAME},QUEUE_URL=${QUEUE_URL},RETRY_DELAY_SECONDS=${RETRY_DELAY_SECONDS},LOCK_TTL_SECONDS=${LOCK_TTL_SECONDS}"
if [ -n "$TABLE_ALLOWLIST" ]; then
  ENV_VARS="${ENV_VARS},TABLE_ALLOWLIST=${TABLE_ALLOWLIST}"
fi

# 2d) Attach policy for DynamoDB + SQS
cat > "$TMPDIR/lambda_ddb_sqs_policy.json" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["dynamodb:PutItem"],
      "Resource": "arn:aws:dynamodb:${AWS_REGION}:*:table/${LOCK_TABLE_NAME}"
    },
    {
      "Effect": "Allow",
      "Action": ["sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
      "Resource": "${QUEUE_ARN}"
    }
  ]
}
JSON

aws iam put-role-policy \
  --role-name "$LAMBDA_ROLE_NAME" \
  --policy-name iceberg-delete-detector-ddb-sqs \
  --policy-document file://"$TMPDIR/lambda_ddb_sqs_policy.json"

# 3) Package Lambda
cp "$PROJECT_ROOT/scripts/lambda_detect_delete.py" "$TMPDIR/lambda_function.py"
( cd "$TMPDIR" && zip -q lambda.zip lambda_function.py )

# 4) Create or update Lambda function
FUNCTION_ARN=$(aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" \
  --query Configuration.FunctionArn --output text 2>/dev/null || true)

if [ -z "$FUNCTION_ARN" ] || [ "$FUNCTION_ARN" = "None" ]; then
  FUNCTION_ARN=$(aws lambda create-function \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --runtime python3.11 \
    --role "$ROLE_ARN" \
    --handler lambda_function.handler \
    --zip-file fileb://"$TMPDIR/lambda.zip" \
    --timeout 30 \
    --environment "Variables={$ENV_VARS}" \
    --query FunctionArn --output text)
else
  wait_lambda_ready() {
    local status="InProgress"
    while [ "$status" = "InProgress" ]; do
      status=$(aws lambda get-function-configuration \
        --function-name "$LAMBDA_FUNCTION_NAME" \
        --query 'LastUpdateStatus' --output text)
      if [ "$status" = "InProgress" ]; then
        sleep 5
      fi
    done
  }

  wait_lambda_ready
  aws lambda update-function-code \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --zip-file fileb://"$TMPDIR/lambda.zip" >/dev/null

  wait_lambda_ready
  aws lambda update-function-configuration \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --environment "Variables={$ENV_VARS}" \
    --timeout 30 >/dev/null
fi

# 5) Create or update EventBridge rule
cat > "$TMPDIR/event_rule.json" <<JSON
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {"name": ["${BUCKET_NAME}"]},
    "object": {"key": [{"suffix": "${DELETE_SUFFIX}"}]}
  }
}
JSON

aws events put-rule \
  --region "$AWS_REGION" \
  --name "$EVENT_RULE_NAME" \
  --event-pattern file://"$TMPDIR/event_rule.json" \
  --state ENABLED >/dev/null

# 6) Allow EventBridge to invoke Lambda
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
aws lambda add-permission \
  --function-name "$LAMBDA_FUNCTION_NAME" \
  --statement-id "AllowEventBridgeInvoke" \
  --action "lambda:InvokeFunction" \
  --principal "events.amazonaws.com" \
  --source-arn "arn:aws:events:${AWS_REGION}:${ACCOUNT_ID}:rule/${EVENT_RULE_NAME}" \
  2>/dev/null || true

# 7) Set Lambda as target
aws events put-targets \
  --region "$AWS_REGION" \
  --rule "$EVENT_RULE_NAME" \
  --targets "Id"="lambda-target","Arn"="$FUNCTION_ARN" >/dev/null

# 8) Wire SQS -> Lambda for retries
aws lambda create-event-source-mapping \
  --function-name "$LAMBDA_FUNCTION_NAME" \
  --event-source-arn "$QUEUE_ARN" \
  --batch-size 1 \
  --maximum-batching-window-in-seconds 0 \
  --region "$AWS_REGION" >/dev/null 2>&1 || true

cat <<OUT
Deployed:
- Lambda: $FUNCTION_ARN
- Event rule: $EVENT_RULE_NAME
- Bucket: $BUCKET_NAME
- Delete suffix: $DELETE_SUFFIX
- Lock table: $LOCK_TABLE_NAME
- Retry queue: $QUEUE_URL
OUT
