/*
rule = GroupingIdTypeDetect
 */
package fix

import org.apache.spark.sql.functions._

object GroupingIdTypeDetectExample {
  val bad = grouping_id() // assert: GroupingIdTypeDetect
}
