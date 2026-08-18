/*
rule = SqlInStringDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object SqlInStringDetectExample {
  def run(spark: SparkSession, table: String): Unit = {
    spark.sql("SELECT * FROM people") // assert: SqlInStringDetect
    spark.sql(s"SELECT * FROM $table") // assert: SqlInStringDetect
    spark.sql(buildQuery()) // assert: SqlInStringDetect
  }

  def buildQuery(): String = "SELECT 1"
}
