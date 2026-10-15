/*
rule = DateTimeStringComparisonDetect
 */
package fix

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DateTimeStringComparisonDetectExample {
  val cutoff = "2020-01-01"
  val start = lit("2019-12-01 00:00:00")

  def run(spark: SparkSession, events: DataFrame): Unit = {
    import spark.implicits._

    // Not zero-padded, or with a time part: changes under every operator.
    val recent = events.filter(col("start_date") >= "2017-7-1") // assert: DateTimeStringComparisonDetect
    val sameDay = events.where($"event_date" === "2020-1-1") // assert: DateTimeStringComparisonDetect
    val notThen = events.filter(events("event_ts") =!= lit("2020-01-15 10:00:00")) // assert: DateTimeStringComparisonDetect
    val sinceStart = events.filter(col("event_ts").geq(start)) // assert: DateTimeStringComparisonDetect

    // A zero-padded date: only > and <=, and between()'s upper bound.
    val after = events.filter(col("event_ts") > "2020-01-01") // assert: DateTimeStringComparisonDetect
    val upTo = events.filter(col("event_ts").leq("2020-01-31")) // assert: DateTimeStringComparisonDetect
    val window = events.filter(col("event_ts").between("2020-01-01", "2020-01-31")) // assert: DateTimeStringComparisonDetect
    val reversed = events.filter(lit("2020-01-01") >= col("event_ts")) // assert: DateTimeStringComparisonDetect
    val sinceCutoff = events.filter(col("event_ts") > cutoff) // assert: DateTimeStringComparisonDetect

    // Not flagged: the same answer on both versions, an explicit conversion,
    // not a date-shaped string, or not a Column comparison.
    val today = events.filter(col("event_date") === "2020-01-01")
    val fromCutoff = events.filter(col("event_date") >= cutoff)
    val before = events.filter(col("event_date") < "2020-02-01")
    val reversedSame = events.filter(lit("2020-01-01") <= col("event_ts"))
    val converted = events.filter(col("event_date") > to_date(lit("2020-01-01")))
    val byStatus = events.filter(col("status") === "ACTIVE")
    val plainStrings = cutoff > "2019-06-01"
  }
}
