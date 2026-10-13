/*
rule = AddMonthsSnapDetect
 */
package fix

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AddMonthsSnapDetectExample {
  def shapes(spark: SparkSession, df: DataFrame, other: DataFrame, d: Column, n: Int): Unit = {
    import spark.implicits._
    // Rewritten: the auto-generated column name is discarded.
    df.select(add_months(col("dt"), 1).as("next")) // assert: AddMonthsSnapDetect
    df.select(add_months(column("dt"), -1).alias("prev")) // assert: AddMonthsSnapDetect
    df.select(add_months($"dt", n).cast("string").name("s")) // assert: AddMonthsSnapDetect
    df.withColumn("next", add_months('dt, 1)) // assert: AddMonthsSnapDetect
    df.withColumn("next", add_months(df("dt"), 1).cast("string")) // assert: AddMonthsSnapDetect
    df.filter(add_months(df.col("dt"), 1) > lit("2019-03-01")) // assert: AddMonthsSnapDetect
    df.where(add_months(d, n) === current_date()) // assert: AddMonthsSnapDetect
    df.orderBy(add_months(to_date(col("dt")), 1)) // assert: AddMonthsSnapDetect
    df.join(other, add_months(to_timestamp($"ts", "yyyy-MM-dd HH:mm"), 1) === other("d")) // assert: AddMonthsSnapDetect
    df.select(when(add_months(col("dt"), 1) > lit("2019-03-01"), 1).otherwise(0).as("flag")) // assert: AddMonthsSnapDetect

    // Safe arguments, but the result may become a named output column whose
    // name the rewrite would change: reported as AddMonthsSnapDetect.Manual.
    df.select(add_months(col("dt"), 1)) // assert: AddMonthsSnapDetect
    df.select(when(add_months(col("dt"), 1) > lit("2019-03-01"), 1)) // assert: AddMonthsSnapDetect
    df.groupBy(add_months(col("dt"), 1)).count() // assert: AddMonthsSnapDetect
    val held = add_months(col("dt"), 1) // assert: AddMonthsSnapDetect

    // Not safe to repeat: reported as AddMonthsSnapDetect.Manual and left as written.
    df.select(add_months(coalesce(col("a"), col("b")), 1).as("x")) // assert: AddMonthsSnapDetect
    df.select(add_months(col("dt"), n * 12).as("x")) // assert: AddMonthsSnapDetect

    // Unaffected: last_day of the result is the same on both versions.
    val alreadyLastDay = last_day(add_months(col("dt"), 1))
    val ok = date_add(col("dt"), 1)
  }

  // The rule's own output is not rewritten again.
  def rewritten(d: Column): Column =
    when(to_date(d) === last_day(d), last_day(add_months(d, 1))).otherwise(add_months(d, 1)).as("x")
}
