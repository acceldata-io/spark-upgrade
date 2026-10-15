/*
rule = DateTimeFormatPatternValidator
 */
package fix

import org.apache.spark.sql.functions._

object DateTimeFormatPatternValidatorExample {
  val bad1 = to_date(col("d"), "dd/MM/yyyy hh:mm") // assert: DateTimeFormatPatternValidator
  val bad2 = date_format(col("d"), "F") // assert: DateTimeFormatPatternValidator
  val weekYear = date_format(col("d"), "YYYY-MM-dd") // assert: DateTimeFormatPatternValidator
  val weekOfYear = to_date(col("d"), "yyyy-ww") // assert: DateTimeFormatPatternValidator
  val dayNumber = date_format(col("d"), "u") // assert: DateTimeFormatPatternValidator
  val parseDayName = to_timestamp(col("d"), "EEE, dd MMM yyyy") // assert: DateTimeFormatPatternValidator
  val narrowMonth = date_format(col("d"), "MMMMM") // assert: DateTimeFormatPatternValidator
  val padded = from_unixtime(col("t"), "yyyy-MM-dd HHH") // assert: DateTimeFormatPatternValidator
  val optional = unix_timestamp(col("d"), "yyyy-MM-dd[ HH:mm]") // assert: DateTimeFormatPatternValidator
  val reserved = date_format(col("d"), "yyyy#MM") // assert: DateTimeFormatPatternValidator

  // Unchanged on 3.5.5 (probed): formatting with E, h with a, quoted letters.
  val ok = date_format(col("d"), "yyyy-MM-dd HH:mm:ss")
  val okName = date_format(col("d"), "EEE, dd MMM yyyy")
  val okClock = to_timestamp(col("d"), "yyyy-MM-dd hh:mm a")
  val okQuoted = to_timestamp(col("d"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
  val okMonth = to_date(col("d"), "dd-MMM-yy")
}
