/*
rule = IsRunningLocallyWarn
 */
package fix

import org.apache.spark.TaskContext

// Input-only fixture: a receiver that may have a side effect is reported as
// IsRunningLocallyWarn.Manual and left as written. TaskContext.isRunningLocally
// was REMOVED in Spark 3.0, so the `output` project cannot reference it.
object IsRunningLocallyManual {
  def make(): TaskContext = TaskContext.get()
  def viaCall(): Boolean = make().isRunningLocally()// assert: IsRunningLocallyWarn
  def viaLambda(o: Option[TaskContext]): Option[Boolean] = o.map(_.isRunningLocally())// assert: IsRunningLocallyWarn
}
