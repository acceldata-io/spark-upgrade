package fix

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_utc_timestamp}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}
import org.apache.spark.unsafe.types.CalendarInterval

object NewDetectors {

  def fromFirst(spark: SparkSession): Unit = {
    spark.sql("FROM events")
    spark.sql("from events select id")
    // a normal query, and a table whose name merely starts with "from"
    spark.sql("SELECT id FROM fromage")
  }

  def timezones(spark: SparkSession): Unit = {
    val df = spark.range(1).toDF("id")
    // PDT is not a resolvable zone id -- Spark 2.4 fell back to GMT
    df.select(from_utc_timestamp(col("id"), "PDT"))
    // EST resolves via ZoneId.SHORT_IDS, so it must NOT be flagged
    df.select(from_utc_timestamp(col("id"), "EST"))
    df.select(from_utc_timestamp(col("id"), "America/New_York"))
  }

  def intervals(ci: CalendarInterval): String = {
    ci.toString
  }

  def jsonWithSchema(spark: SparkSession): Unit = {
    val schema = StructType(Seq(StructField("n", IntegerType)))
    spark.read.schema(schema).json("/tmp/in.json")
    // inferred schema -- nothing to reject, must NOT be flagged
    spark.read.json("/tmp/in.json")
  }

  def csvWithBinary(spark: SparkSession): Unit = {
    val schema = StructType(Seq(StructField("blob", BinaryType)))
    spark.read.schema(schema).csv("/tmp/in.csv")
  }
}
