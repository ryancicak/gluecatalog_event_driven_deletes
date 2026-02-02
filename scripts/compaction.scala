import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.spark.Spark3Util

val tableIdent = sys.env.getOrElse(
  "TABLE_IDENT",
  "glue_catalog.federation_demo_db_ryan.customers_iceberg",
)
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
  val result = SparkActions.get(spark)
    .rewriteDataFiles(table)
    .option("rewrite-all", "true")
    .option("delete-file-threshold", "1")
    .execute()

  println(
    s"Pass ${pass} result: addedDataFiles=${result.addedDataFilesCount()}, " +
      s"rewrittenDataFiles=${result.rewrittenDataFilesCount()}, " +
      s"rewrittenBytes=${result.rewrittenBytesCount()}, " +
      s"removedDeleteFiles=${result.removedDeleteFilesCount()}, " +
      s"failedDataFiles=${result.failedDataFilesCount()}"
  )

  val newCount = deleteFileCount()
  println(s"Delete files after pass ${pass}: ${newCount}")
  if (newCount >= lastCount) {
    println("Delete file count did not decrease; stopping further passes.")
    lastCount = newCount
    pass = maxPasses
  } else {
    lastCount = newCount
  }
}

spark.stop()
System.exit(0)
