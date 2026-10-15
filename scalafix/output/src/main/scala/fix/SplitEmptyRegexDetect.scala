package fix

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._

object SplitEmptyRegexDetectExample {
  def shapes(df: DataFrame): Unit = {
    // Rewritten: the auto-generated column name is discarded.
    df.select(split(col("s"), "(?=)").as("chars")) 
    df.withColumn("chars", split(col("s"), "(?=)")) 
    df.select(size(split(col("s"), "(?=)")).alias("n")) 
    df.filter(size(functions.split(col("s"), "(?=)")) > 2) 

    // The result may become a named output column: SplitEmptyRegexDetect.Manual.
    df.select(split(col("s"), "")) 
    val bad = split(col("s"), "") 

    // Not affected: a non-empty regex.
    val ok = split(col("s"), ",")
    df.select(split(col("s"), "(?=)").as("chars"))
  }
}
