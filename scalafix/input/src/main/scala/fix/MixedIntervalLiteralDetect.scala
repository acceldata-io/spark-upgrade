/*
rule = MixedIntervalLiteralDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object MixedIntervalLiteralDetectExample {
  def run(spark: SparkSession): Unit = {
    spark.sql("SELECT INTERVAL '1' YEAR '2' DAY") // assert: MixedIntervalLiteralDetect
    spark.sql("SELECT INTERVAL '1' YEAR '2' MONTH")

    // The mixed clause is the SECOND one -- only checking the first clause
    // in the string used to miss this.
    spark.sql("SELECT INTERVAL '1' YEAR, INTERVAL '2' YEAR '3' DAY") // assert: MixedIntervalLiteralDetect
  }
}
