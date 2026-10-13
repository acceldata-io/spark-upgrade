package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector (design review Part 2 Family G): Spark 3.5 introduces
 * `EnhancedAnalysisException` and reshapes how a failed analysis's plan is
 * exposed on the caught exception. No exact rewrite exists: probed on 2.4.8
 * and 3.5.5, reading the plan through ExtendedAnalysisException matches 2.4's
 * plan.isDefined for unresolved columns, type mismatches and the like, but a
 * missing table (spark.table, SELECT ... FROM, insertInto) has a plan on 3.5.5
 * where 2.4.8 had None. Detect-only.
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
            "No auto-rewrite. On 3.x the plan is on org.apache.spark.sql.catalyst.ExtendedAnalysisException: " +
              "e match { case x: ExtendedAnalysisException => x.plan; case _ => None }. That is not always 2.4's answer " +
              "(probed): a missing table now carries a plan where 2.4.8 had None.",
            t
          )
        )
    }.asPatch
  }
}
