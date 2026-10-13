/*
rule = AddMonthsSnapDetect
 */
package fix

import org.apache.spark.sql.{Column, functions => F}
import org.apache.spark.sql.functions.{add_months, col}

object AddMonthsSnapImports {
  // A bare call with named imports: when/to_date/last_day are imported.
  val bare: Column = add_months(col("dt"), 1).as("next") // assert: AddMonthsSnapDetect
  // A qualified call reuses its qualifier; no import needed.
  val qualified: Column = F.add_months(F.col("dt"), 1).as("next") // assert: AddMonthsSnapDetect
  val fullyQualified: Column = org.apache.spark.sql.functions.add_months(col("dt"), 1).as("next") // assert: AddMonthsSnapDetect
}
