/*
rule = MixedIntervalLiteralDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object MixedIntervalLiteralDetectExample {
  def run(spark: SparkSession): Unit = {
    spark.sql("SELECT INTERVAL '1' YEAR '2' DAY") // assert: MixedIntervalLiteralDetect
    spark.sql("SELECT INTERVAL '1' YEAR '2' MONTH")
  }
}
