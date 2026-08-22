/*
rule = AddMonthsSnapDetect
 */
package fix

import org.apache.spark.sql.functions._

object AddMonthsSnapDetectExample {
  val bad = add_months(col("dt"), 1) // assert: AddMonthsSnapDetect
  val ok = date_add(col("dt"), 1)
}
