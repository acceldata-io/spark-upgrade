/*
rule = UntypedScalaUDFDetect
 */
package fix

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object UntypedScalaUDFDetectExample {
  val untyped = udf((x: Int) => x, IntegerType) // assert: UntypedScalaUDFDetect
  val typed = udf((x: Int) => x + 1)
}
