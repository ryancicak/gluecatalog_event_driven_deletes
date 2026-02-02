# Operations

## Manual run
1) Copy compaction.scala to EMR master:
   scp -i <key.pem> scripts/compaction.scala hadoop@<emr-master>:/home/hadoop/compaction.scala
2) Run:
   spark-shell (with Iceberg configs) -i /home/hadoop/compaction.scala

## Validation
- Check Iceberg metadata tables:
  SELECT * FROM glue_catalog.<db>.<table>.delete_files;
  SELECT * FROM glue_catalog.<db>.<table>.files;
