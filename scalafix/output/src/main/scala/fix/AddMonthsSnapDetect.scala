package fix

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AddMonthsSnapDetectExample {
  def shapes(spark: SparkSession, df: DataFrame, other: DataFrame, d: Column, n: Int): Unit = {
    import spark.implicits._
    // Rewritten: the auto-generated column name is discarded.
    df.select(when(to_date(col("dt")) === last_day(col("dt")), last_day(add_months(col("dt"), 1))).otherwise(add_months(col("dt"), 1)).as("next")) 
    df.select(when(to_date(column("dt")) === last_day(column("dt")), last_day(add_months(column("dt"), -1))).otherwise(add_months(column("dt"), -1)).alias("prev")) 
    df.select(when(to_date($"dt") === last_day($"dt"), last_day(add_months($"dt", n))).otherwise(add_months($"dt", n)).cast("string").name("s")) 
    df.withColumn("next", when(to_date('dt) === last_day('dt), last_day(add_months('dt, 1))).otherwise(add_months('dt, 1))) 
    df.withColumn("next", when(to_date(df("dt")) === last_day(df("dt")), last_day(add_months(df("dt"), 1))).otherwise(add_months(df("dt"), 1)).cast("string")) 
    df.filter(when(to_date(df.col("dt")) === last_day(df.col("dt")), last_day(add_months(df.col("dt"), 1))).otherwise(add_months(df.col("dt"), 1)) > lit("2019-03-01")) 
    df.where(when(to_date(d) === last_day(d), last_day(add_months(d, n))).otherwise(add_months(d, n)) === current_date()) 
    df.orderBy(when(to_date(to_date(col("dt"))) === last_day(to_date(col("dt"))), last_day(add_months(to_date(col("dt")), 1))).otherwise(add_months(to_date(col("dt")), 1))) 
    df.join(other, when(to_date(to_timestamp($"ts", "yyyy-MM-dd HH:mm")) === last_day(to_timestamp($"ts", "yyyy-MM-dd HH:mm")), last_day(add_months(to_timestamp($"ts", "yyyy-MM-dd HH:mm"), 1))).otherwise(add_months(to_timestamp($"ts", "yyyy-MM-dd HH:mm"), 1)) === other("d")) 
    df.select(when(when(to_date(col("dt")) === last_day(col("dt")), last_day(add_months(col("dt"), 1))).otherwise(add_months(col("dt"), 1)) > lit("2019-03-01"), 1).otherwise(0).as("flag")) 

    // Safe arguments, but the result may become a named output column whose
    // name the rewrite would change: reported as AddMonthsSnapDetect.Manual.
    df.select(add_months(col("dt"), 1)) 
    df.select(when(add_months(col("dt"), 1) > lit("2019-03-01"), 1)) 
    df.groupBy(add_months(col("dt"), 1)).count() 
    val held = add_months(col("dt"), 1) 

    // Not safe to repeat: reported as AddMonthsSnapDetect.Manual and left as written.
    df.select(add_months(coalesce(col("a"), col("b")), 1).as("x")) 
    df.select(add_months(col("dt"), n * 12).as("x")) 

    // Unaffected: last_day of the result is the same on both versions.
    val alreadyLastDay = last_day(add_months(col("dt"), 1))
    val ok = date_add(col("dt"), 1)
  }

  // The rule's own output is not rewritten again.
  def rewritten(d: Column): Column =
    when(to_date(d) === last_day(d), last_day(add_months(d, 1))).otherwise(add_months(d, 1)).as("x")
}
