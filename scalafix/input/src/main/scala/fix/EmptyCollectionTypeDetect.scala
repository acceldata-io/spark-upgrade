/*
rule = EmptyCollectionTypeDetect
 */
package fix

import org.apache.spark.sql.functions._

object EmptyCollectionTypeDetectExample {
  val bad1 = array() // assert: EmptyCollectionTypeDetect
  val bad2 = map() // assert: EmptyCollectionTypeDetect
  val ok = array(lit(1), lit(2))
}
