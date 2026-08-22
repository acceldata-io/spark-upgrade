/*
rule = AnalysisExceptionPlanFieldDetect
 */
package fix

import org.apache.spark.sql.AnalysisException

object AnalysisExceptionPlanFieldDetectExample {
  def handle(e: AnalysisException): Unit = {
    val bad = e.plan // assert: AnalysisExceptionPlanFieldDetect
    val ok = e.getMessage
  }
}
