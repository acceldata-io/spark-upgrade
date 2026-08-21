/*
rule = CountStarWithTableIdentDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object CountStarWithTableIdentDetectExample {
  def run(spark: SparkSession): Unit = {
    spark.sql("SELECT count(t.*) FROM t") // assert: CountStarWithTableIdentDetect
    spark.sql("SELECT count(*) FROM t")
  }
}
