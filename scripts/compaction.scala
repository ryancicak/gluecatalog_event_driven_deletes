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
