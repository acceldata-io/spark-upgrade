/*
rule = SplitEmptyRegexDetect
 */
package fix

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._

object SplitEmptyRegexDetectExample {
  def shapes(df: DataFrame): Unit = {
    // Rewritten: the auto-generated column name is discarded.
    df.select(split(col("s"), "").as("chars")) // assert: SplitEmptyRegexDetect
    df.withColumn("chars", split(col("s"), "")) // assert: SplitEmptyRegexDetect
    df.select(size(split(col("s"), "")).alias("n")) // assert: SplitEmptyRegexDetect
    df.filter(size(functions.split(col("s"), "")) > 2) // assert: SplitEmptyRegexDetect

    // The result may become a named output column: SplitEmptyRegexDetect.Manual.
    df.select(split(col("s"), "")) // assert: SplitEmptyRegexDetect
    val bad = split(col("s"), "") // assert: SplitEmptyRegexDetect

    // Not affected: a non-empty regex.
    val ok = split(col("s"), ",")
    df.select(split(col("s"), "(?=)").as("chars"))
  }
}
