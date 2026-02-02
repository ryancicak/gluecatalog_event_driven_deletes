# EMR footprint + scale-up option

You can keep a small EMR cluster running and scale for compaction runs if needed.

## Option A: Keep small footprint (recommended for demo)
- Run compaction on the small cluster
- Simpler and predictable

## Option B: Scale up before compaction (optional)
You can add a Step Functions state to modify instance groups before the EMR step, then scale back after.

Example API calls:
- `ModifyInstanceGroups` to increase core/task capacity
- `ModifyInstanceGroups` to reduce after the step completes

This can be added to the Step Functions definition if needed.
