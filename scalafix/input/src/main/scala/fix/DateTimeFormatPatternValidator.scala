/*
rule = DateTimeFormatPatternValidator
 */
package fix

import org.apache.spark.sql.functions._

object DateTimeFormatPatternValidatorExample {
  val bad1 = to_date(col("d"), "dd/MM/yyyy hh:mm") // assert: DateTimeFormatPatternValidator
  val bad2 = date_format(col("d"), "F") // assert: DateTimeFormatPatternValidator
  val ok = date_format(col("d"), "yyyy-MM-dd HH:mm:ss")
}
