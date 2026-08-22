/*
rule = NaFunctionsNameMatchDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object NaFunctionsNameMatchDetectExample {
  def run(spark: SparkSession): Unit = {
    val df = spark.read.format("parquet").load("/data/a")
    val bad = df.na.replace("id", Map(1 -> 2)) // assert: NaFunctionsNameMatchDetect
    val alsoBad = df.na.fill("unknown", Seq("category")) // assert: NaFunctionsNameMatchDetect
    val ok = df.filter(df("id").isNotNull)
  }
}
