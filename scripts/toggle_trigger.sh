#!/usr/bin/env bash
set -euo pipefail

# Toggle the EventBridge trigger on/off
# Usage: ./toggle_trigger.sh [on|off|status]

AWS_REGION="${AWS_REGION:-us-west-2}"
EVENT_RULE_NAME="${EVENT_RULE_NAME:-iceberg-delete-file-events}"

ACTION="${1:-status}"

case "$ACTION" in
  on|enable)
    aws events enable-rule --name "$EVENT_RULE_NAME" --region "$AWS_REGION"
    echo "✅ Trigger ENABLED: $EVENT_RULE_NAME"
    ;;
  off|disable)
    aws events disable-rule --name "$EVENT_RULE_NAME" --region "$AWS_REGION"
    echo "🔴 Trigger DISABLED: $EVENT_RULE_NAME"
    ;;
  status)
    STATE=$(aws events describe-rule --name "$EVENT_RULE_NAME" --region "$AWS_REGION" --query 'State' --output text)
    if [ "$STATE" = "ENABLED" ]; then
      echo "✅ Trigger is ENABLED"
    else
      echo "🔴 Trigger is DISABLED"
    fi
    ;;
  *)
    echo "Usage: $0 [on|off|status]"
    echo "  on/enable   - Enable the EventBridge trigger"
    echo "  off/disable - Disable the EventBridge trigger"
    echo "  status      - Check current state"
    exit 1
    ;;
esac
