/*
rule = HashOnMapTypeDetect
 */
package fix

import org.apache.spark.sql.functions._

object HashOnMapTypeDetectExample {
  val bad = hash(map(lit("k"), lit("v"))) // assert: HashOnMapTypeDetect
  val ok = hash(col("id"))
}
