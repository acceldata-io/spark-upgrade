/*
rule = PathOptionConflictDetect
 */
package fix

import org.apache.spark.sql.{DataFrame, SparkSession}

object PathOptionConflictDetectExample {
  def run(spark: SparkSession, df: DataFrame, p: String): Unit = {
    // Rewritten: one path argument, which 2.4 used; the option is dropped.
    val bad = spark.read.option("path", "/data/a").format("parquet").load("/data/a") // assert: PathOptionConflictDetect
    val multiLine = spark.read // assert: PathOptionConflictDetect
      .format("csv")
      .option("path", p)
      .option("header", "true")
      .load("/data/x")
    df.write.option("path", p).format("parquet").mode("overwrite").save("/data/out") // assert: PathOptionConflictDetect

    // The .options(Map(...)) overload: the "path" pair goes, or the whole call when it is the only pair.
    val badMap = spark.read.options(Map("path" -> "/data/c")).format("parquet").load("/data/c") // assert: PathOptionConflictDetect
    val badMap2 = spark.read.options(Map("header" -> "true", "path" -> p)).format("csv").load("/data/c") // assert: PathOptionConflictDetect

    // Reported as PathOptionConflictDetect.Config and left as written.
    val multi = spark.read.option("path", p).format("parquet").load("/data/a", "/data/b") // assert: PathOptionConflictDetect
    val sideEffect = spark.read.option("path", p.trim()).format("parquet").load("/data/a") // assert: PathOptionConflictDetect

    val ok = spark.read.format("parquet").load("/data/b")
    // Not at risk: "path" appears only as another key's VALUE, not as a key.
    val okMap = spark.read.options(Map("tag" -> "path")).format("parquet").load("/data/d")
  }
}
