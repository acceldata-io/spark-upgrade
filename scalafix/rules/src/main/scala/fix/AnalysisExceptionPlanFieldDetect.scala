package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector (design review Part 2 Family G): Spark 3.5 introduces
 * `EnhancedAnalysisException` and reshapes how a failed analysis's plan is
 * exposed on the caught exception. No safe mechanical rewrite exists (the
 * replacement depends on how the catch site uses the plan), so this is
 * detect-only.
 */
class AnalysisExceptionPlanFieldDetect extends SemanticRule("AnalysisExceptionPlanFieldDetect") {
  override val description =
    "Flags AnalysisException.plan field access -- Spark 3.5 reshapes how a failed analysis's plan is exposed on the caught exception."

  private val planFieldMatcher = SymbolMatcher.normalized("org.apache.spark.sql.AnalysisException.plan")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Select(_, planFieldMatcher(_)) =>
        RuleFinding.report(
          RuleChange(
            "AnalysisExceptionPlanFieldDetect",
            "AnalysisException.plan accessed -- Spark 3.5 introduces EnhancedAnalysisException and reshapes how a failed analysis's plan is exposed.",
            "No auto-rewrite; review this exception-handling code against the 3.5 migration notes for AnalysisException.",
            t
          )
        )
    }.asPatch
  }
}
