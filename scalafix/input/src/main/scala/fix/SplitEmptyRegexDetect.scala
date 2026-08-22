/*
rule = SplitEmptyRegexDetect
 */
package fix

import org.apache.spark.sql.functions._

object SplitEmptyRegexDetectExample {
  val bad = split(col("s"), "") // assert: SplitEmptyRegexDetect
  val ok = split(col("s"), ",")
}
