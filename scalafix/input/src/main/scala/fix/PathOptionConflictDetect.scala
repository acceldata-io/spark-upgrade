/*
rule = PathOptionConflictDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object PathOptionConflictDetectExample {
  def run(spark: SparkSession): Unit = {
    val bad = spark.read.option("path", "/data/a").format("parquet").load("/data/a") // assert: PathOptionConflictDetect
    val ok = spark.read.format("parquet").load("/data/b")
  }
}
