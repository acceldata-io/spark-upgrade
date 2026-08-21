/*
rule = ExponentLiteralDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object ExponentLiteralDetectExample {
  def run(spark: SparkSession): Unit = {
    spark.sql("SELECT 1E2 AS x") // assert: ExponentLiteralDetect
    spark.sql("SELECT 100 AS x")
  }
}
