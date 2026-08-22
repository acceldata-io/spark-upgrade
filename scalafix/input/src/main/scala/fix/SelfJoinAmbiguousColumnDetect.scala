/*
rule = SelfJoinAmbiguousColumnDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object SelfJoinAmbiguousColumnDetectExample {
  def run(spark: SparkSession): Unit = {
    val df = spark.read.format("parquet").load("/data/a")
    val bad = df.join(df, "id") // assert: SelfJoinAmbiguousColumnDetect
    val ok = df.join(spark.read.format("parquet").load("/data/b"), "id")
  }
}
