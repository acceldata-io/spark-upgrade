package fix

import org.apache.spark.TaskContext

// Rewritten: the receiver has no side effect, and 2.4.8's isRunningLocally
// always returned false.
object IsRunningLocallyUsage {
  def localCheck(): Boolean = {
    false
  }

  def guarded(ctx: TaskContext): Int = {
    val tc = TaskContext.get
    if (false) 1 else if (!false) 2 else 3
  }

  def noParens(): Boolean = false
}
