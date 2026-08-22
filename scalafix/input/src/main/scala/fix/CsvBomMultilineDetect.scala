/*
rule = CsvBomMultilineDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object CsvBomMultilineDetectExample {
  def run(spark: SparkSession): Unit = {
    val bad = spark.read.option("multiLine", "true").csv("/data/a") // assert: CsvBomMultilineDetect
    val ok = spark.read.option("multiLine", "true").option("encoding", "UTF-8").csv("/data/b")
    val alsoOk = spark.read.csv("/data/c")
  }
}
