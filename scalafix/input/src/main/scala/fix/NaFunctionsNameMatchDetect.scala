/*
rule = NaFunctionsNameMatchDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object NaFunctionsNameMatchDetectExample {
  def run(spark: SparkSession): Unit = {
    val df = spark.read.format("parquet").load("/data/a")
    val bad = df.na.replace("id", Map(1 -> 2)) // assert: NaFunctionsNameMatchDetect
    val alsoBad = df.na.replace(Seq("a", "b"), Map("x" -> "y")) // assert: NaFunctionsNameMatchDetect
    // Not flagged: fill resolved names the same way on 2.4.8; "*" is every column.
    val fill = df.na.fill("unknown", Seq("category"))
    val all = df.na.replace("*", Map(1 -> 2))
    val ok = df.filter(df("id").isNotNull)
  }
}
