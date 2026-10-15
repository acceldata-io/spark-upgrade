/*
rule = CommandOutputSchemaDetect
 */
package fix

import org.apache.spark.sql.{SQLContext, SparkSession}

object CommandOutputSchemaDetectExample {
  def run(spark: SparkSession, sqlContext: SQLContext, db: String): Unit = {
    val dbs = spark.sql("SHOW DATABASES").collect().map(_.getAs[String]("databaseName")) // assert: CommandOutputSchemaDetect
    spark.sql("show schemas like 'sales*'") // assert: CommandOutputSchemaDetect
    sqlContext.sql("SHOW TABLES IN sales") // assert: CommandOutputSchemaDetect
    spark.sql("SHOW TABLE EXTENDED IN sales LIKE 'orders'") // assert: CommandOutputSchemaDetect
    spark.sql( // assert: CommandOutputSchemaDetect
      """
        |DESCRIBE DATABASE EXTENDED sales""".stripMargin)
    spark.sql("desc schema sales") // assert: CommandOutputSchemaDetect
    spark.sql(s"SHOW TABLES IN $db") // assert: CommandOutputSchemaDetect

    // Not affected: other commands, a table description, and SQL the rule cannot read.
    val query = "SHOW DATABASES"
    spark.sql(query)
    spark.sql("SHOW PARTITIONS sales.orders")
    spark.sql("DESCRIBE TABLE sales.orders")
    spark.sql("SELECT 'SHOW DATABASES' AS s")
  }
}
