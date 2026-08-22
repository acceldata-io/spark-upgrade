/*
rule = SetCommandSparkConfDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object SetCommandSparkConfDetectExample {
  def run(spark: SparkSession): Unit = {
    spark.sql("SET spark.executor.memory=4g") // assert: SetCommandSparkConfDetect
    spark.sql("SET spark.sql.shuffle.partitions=200")
  }
}
