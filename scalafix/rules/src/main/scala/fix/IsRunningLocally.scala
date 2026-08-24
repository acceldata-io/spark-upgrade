package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Core migration guide, 2.4 -> 3.0: `TaskContext.isRunningLocally()` was
 * removed along with local execution. Verified present in the real
 * `spark-core_2.11-2.4.8.jar`, so a 2.4.8 codebase can genuinely be calling it.
 *
 * Detect-only: the right fix is almost always to delete the branch the call
 * guards (local execution no longer exists, so the condition is dead), and
 * which branch to keep is a judgement a human has to make.
 *
 * This rule was already written and correct but had two problems that made it
 * dead code: it was absent from `META-INF/services/scalafix.v1.Rule`, so it was
 * never enabled at all, and it reported via a raw `Patch.lint` rather than
 * `RuleFinding.report`, so even when run it produced no structured finding for
 * findings.json. Both fixed together -- wiring only one of them would have
 * recreated the silent-under-reporting bug (2026-08-22 review SS5.3).
 */
class IsRunningLocallyWarn extends SemanticRule("IsRunningLocallyWarn") {
  override val description =
    "TaskContext.isRunningLocally() was removed in Spark 3.0; local execution no longer exists, so the code path it guards is dead."

  private val matcher = SymbolMatcher.normalized("org.apache.spark.TaskContext.isRunningLocally")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t: Term.Name if t.value == "isRunningLocally" && matcher.matches(t) =>
        RuleFinding.report(
          RuleChange(
            "IsRunningLocallyWarn",
            "TaskContext.isRunningLocally has been removed in Spark 3.0 -- local execution was removed, so this " +
              "condition can no longer be true. See https://spark.apache.org/docs/3.0.0/core-migration-guide.html",
            "No auto-rewrite; the branch this guards is dead under Spark 3.x and can most likely be deleted.",
            t
          )
        )
    }.asPatch
  }
}
