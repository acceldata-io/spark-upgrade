package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Core migration guide, 2.4 -> 3.0: `TaskContext.isRunningLocally()` was
 * removed along with local execution. Verified present in the real
 * `spark-core_2.11-2.4.8.jar`, so a 2.4.8 codebase can genuinely be calling it.
 *
 * Tier 1 since 2026-10-13: 2.4.8's body is `return false` (javap:
 * `iconst_0; ireturn`), so the call is replaced with `false`, which is 2.4's
 * result on every version (probed on 2.4.8 inside tasks). Only when dropping
 * the receiver cannot drop a side effect: `TaskContext.get()` /
 * `TaskContext.get`, or a val, var or parameter. One difference remains: on
 * the driver, `TaskContext.get()` is null and 2.4 threw a
 * NullPointerException where `false` is returned. A working 2.4 job does not
 * reach that. Any other receiver (a method call, a placeholder, `this`) is
 * reported as IsRunningLocallyWarn.Manual (Tier 3) and left as written.
 */
class IsRunningLocallyWarn extends SemanticRule("IsRunningLocallyWarn") {
  override val description =
    "TaskContext.isRunningLocally() was removed in Spark 3.0; it always returned false in 2.4, so the call is replaced with false."

  private val ruleId = "IsRunningLocallyWarn"
  private val manualId = s"$ruleId.Manual"

  private val matcher = SymbolMatcher.normalized("org.apache.spark.TaskContext.isRunningLocally")
  private val getMatcher = SymbolMatcher.normalized("org.apache.spark.TaskContext.get")

  private val explanation =
    "TaskContext.isRunningLocally was removed in Spark 3.0 together with local execution. In Spark 2.4.8 it always " +
      "returned false. See https://archive.apache.org/dist/spark/docs/3.0.0/core-migration-guide.html"

  private def isStable(n: Term.Name)(implicit doc: SemanticDocument): Boolean =
    n.symbol.info.exists(i => i.isVal || i.isVar || i.isParameter)

  // A receiver whose evaluation has no side effect, so dropping it is exact.
  private def isSafeReceiver(recv: Term)(implicit doc: SemanticDocument): Boolean = recv match {
    case Term.Apply(getMatcher(_), Nil) => true
    case getMatcher(_) => true
    case n: Term.Name => isStable(n)
    case Term.Select(_: Term.Name | _: Term.This, n) => isStable(n)
    case _ => false
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    // The tree that evaluates the call: `recv.isRunningLocally()` or `recv.isRunningLocally`.
    def callOf(sel: Term.Select): Term = sel.parent match {
      case Some(app @ Term.Apply(fun, Nil)) if fun eq sel => app
      case _ => sel
    }
    doc.tree.collect {
      case sel @ Term.Select(recv, name) if name.value == "isRunningLocally" && matcher.matches(name) =>
        val call = callOf(sel)
        val etaExpanded = call.parent.exists(_.isInstanceOf[Term.Eta])
        if (isSafeReceiver(recv) && !etaExpanded) {
          RuleFinding.report(
            RuleChange(ruleId, explanation,
              "Rewrote the call to false, its value in Spark 2.4.8; the branch it guards is dead and can be deleted.", call),
            Patch.replaceTree(call, "false")
          )
        } else {
          RuleFinding.report(
            RuleChange(
              manualId,
              explanation,
              (if (etaExpanded || recv.is[Term.Placeholder])
                s"Not rewritten: `$call` is passed as a function, so it cannot become the constant false. Replace it with " +
                  "a function that returns false; the branch it guards is dead."
              else
                s"Not rewritten: the receiver `$recv` may have side effects, so the tool did not drop it. Replace `$call` " +
                  "with false, keeping the receiver's evaluation if it matters; the branch it guards is dead."),
              call
            )
          )
        }
    }.asPatch
  }
}
