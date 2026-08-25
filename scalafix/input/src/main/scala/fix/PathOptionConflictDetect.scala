/*
rule = PathOptionConflictDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object PathOptionConflictDetectExample {
  def run(spark: SparkSession): Unit = {
    val bad = spark.read.option("path", "/data/a").format("parquet").load("/data/a") // assert: PathOptionConflictDetect
    val ok = spark.read.format("parquet").load("/data/b")

    // Real risk via the .options(Map(...)) overload -- "path" as an actual key.
    val badMap = spark.read.options(Map("path" -> "/data/c")).format("parquet").load("/data/c") // assert: PathOptionConflictDetect

    // Not at risk: "path" appears only as another key's VALUE, not as a key.
    val okMap = spark.read.options(Map("tag" -> "path")).format("parquet").load("/data/d")
  }
}
