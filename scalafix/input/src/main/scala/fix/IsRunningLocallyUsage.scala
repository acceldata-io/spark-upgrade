/*
rule = IsRunningLocallyWarn
 */
package fix

import org.apache.spark.TaskContext

// Input-only fixture, with no `output/` counterpart -- same shape as
// ExecutorPluginWarn's. TaskContext.isRunningLocally was REMOVED in Spark 3.0,
// so the `output` project (which compiles against the target Spark) cannot
// reference it at all. That the output side fails to compile if you try is
// itself the confirmation that the API is genuinely gone.
object IsRunningLocallyUsage {
  def localCheck(): Boolean = {
    TaskContext.get().isRunningLocally()// assert: IsRunningLocallyWarn
  }
}
