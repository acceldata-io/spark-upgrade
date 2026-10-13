package fix

import org.apache.spark.sql.{DataFrame, SparkSession}

object PathOptionConflictDetectExample {
  def run(spark: SparkSession, df: DataFrame, p: String): Unit = {
    // Rewritten: one path argument, which 2.4 used; the option is dropped.
    val bad = spark.read.format("parquet").load("/data/a") 
    val multiLine = spark.read 
      .format("csv")
      .option("header", "true")
      .load("/data/x")
    df.write.format("parquet").mode("overwrite").save("/data/out") 

    // The .options(Map(...)) overload: the "path" pair goes, or the whole call when it is the only pair.
    val badMap = spark.read.format("parquet").load("/data/c") 
    val badMap2 = spark.read.options(Map("header" -> "true")).format("csv").load("/data/c") 

    // Reported as PathOptionConflictDetect.Config and left as written.
    val multi = spark.read.option("path", p).format("parquet").load("/data/a", "/data/b") 
    val sideEffect = spark.read.option("path", p.trim()).format("parquet").load("/data/a") 

    val ok = spark.read.format("parquet").load("/data/b")
    // Not at risk: "path" appears only as another key's VALUE, not as a key.
    val okMap = spark.read.options(Map("tag" -> "path")).format("parquet").load("/data/d")
  }
}
