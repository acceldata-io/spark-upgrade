/*
rule = DuplicateMapKeyLiteralDetect
 */
package fix

import org.apache.spark.sql.functions._

object DuplicateMapKeyLiteralDetectExample {
  val bad = map(lit("a"), lit(1), lit("a"), lit(2)) // assert: DuplicateMapKeyLiteralDetect
  val ok = map(lit("a"), lit(1), lit("b"), lit(2))
}
