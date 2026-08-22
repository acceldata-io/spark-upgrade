/*
rule = MapTypeKeyInCreateMapDetect
 */
package fix

import org.apache.spark.sql.functions._

object MapTypeKeyInCreateMapDetectExample {
  val bad = map(map(lit("k"), lit("v")), lit("x")) // assert: MapTypeKeyInCreateMapDetect
  val ok = map(lit("k"), lit("v"))
}
