package fix

import org.apache.spark.sql.{Column, functions => F}
import org.apache.spark.sql.functions.{add_months, col}
import org.apache.spark.sql.functions.{ last_day, to_date, when }

object AddMonthsSnapImports {
  // A bare call with named imports: when/to_date/last_day are imported.
  val bare: Column = when(to_date(col("dt")) === last_day(col("dt")), last_day(add_months(col("dt"), 1))).otherwise(add_months(col("dt"), 1)).as("next") 
  // A qualified call reuses its qualifier; no import needed.
  val qualified: Column = F.when(F.to_date(F.col("dt")) === F.last_day(F.col("dt")), F.last_day(F.add_months(F.col("dt"), 1))).otherwise(F.add_months(F.col("dt"), 1)).as("next") 
  val fullyQualified: Column = org.apache.spark.sql.functions.when(org.apache.spark.sql.functions.to_date(col("dt")) === org.apache.spark.sql.functions.last_day(col("dt")), org.apache.spark.sql.functions.last_day(org.apache.spark.sql.functions.add_months(col("dt"), 1))).otherwise(org.apache.spark.sql.functions.add_months(col("dt"), 1)).as("next") 
}
