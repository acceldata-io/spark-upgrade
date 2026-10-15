/*
rule = StoreAssignmentPolicyDetect
 */
package fix

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object StoreAssignmentPolicyDetectExample {
  def run(spark: SparkSession, df: DataFrame, mode: String): Unit = {
    spark.sql("INSERT INTO billing.usage SELECT msisdn, date_format(ts, 'yyyy-MM-dd') FROM cdr") // the CLI's SQL linter reports it
    spark.sql("INSERT OVERWRITE TABLE billing.usage PARTITION (d = '2024-01-15') SELECT * FROM cdr") // the CLI's SQL linter reports it
    df.write.insertInto("billing.usage") // assert: StoreAssignmentPolicyDetect
    df.write.mode("append").saveAsTable("billing.usage") // assert: StoreAssignmentPolicyDetect
    df.write.mode(SaveMode.Append).format("parquet").saveAsTable("billing.usage") // assert: StoreAssignmentPolicyDetect
    df.write.mode(mode).saveAsTable("billing.usage") // assert: StoreAssignmentPolicyDetect

    // Not at risk: each of these takes its schema from the query.
    df.write.saveAsTable("billing.fresh")
    df.write.mode("overwrite").saveAsTable("billing.usage")
    df.write.mode(SaveMode.Overwrite).saveAsTable("billing.usage")
    spark.sql("CREATE TABLE billing.copy USING parquet AS SELECT * FROM cdr")
    spark.sql("INSERT OVERWRITE DIRECTORY '/tmp/out' USING parquet SELECT * FROM cdr")
    spark.sql("INSERT OVERWRITE LOCAL DIRECTORY '/tmp/out' SELECT * FROM cdr")
  }
}
