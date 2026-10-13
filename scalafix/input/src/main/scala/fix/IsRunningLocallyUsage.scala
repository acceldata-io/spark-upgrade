/*
rule = IsRunningLocallyWarn
 */
package fix

import org.apache.spark.TaskContext

// Rewritten: the receiver has no side effect, and 2.4.8's isRunningLocally
// always returned false.
object IsRunningLocallyUsage {
  def localCheck(): Boolean = {
    TaskContext.get().isRunningLocally()// assert: IsRunningLocallyWarn
  }

  def guarded(ctx: TaskContext): Int = {
    val tc = TaskContext.get
    if (tc.isRunningLocally) 1 else if (!ctx.isRunningLocally()) 2 else 3// assert: IsRunningLocallyWarn
  }

  def noParens(): Boolean = TaskContext.get.isRunningLocally// assert: IsRunningLocallyWarn
}
