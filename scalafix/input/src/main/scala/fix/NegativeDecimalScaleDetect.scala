/*
rule = NegativeDecimalScaleDetect
 */
package fix

import org.apache.spark.sql.types.DecimalType

object NegativeDecimalScaleDetectExample {
  val bad = DecimalType(38, -2) // assert: NegativeDecimalScaleDetect
  val ok = DecimalType(38, 2)
}
