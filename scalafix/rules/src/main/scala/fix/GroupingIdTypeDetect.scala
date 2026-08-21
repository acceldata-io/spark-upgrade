package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A detector (design review Part 2 Family G / Part 4):
 * `grouping_id()` returns `Long` from Spark 3.0 onward, not `Int`. Reliably
 * telling whether a specific call site's result is later type-matched/
 * compared/stored as `Int` needs data-flow analysis this rule doesn't do --
 * so, like `ExecutorPluginWarn`/`MigrateTrigger` elsewhere in this jar, it
 * flags every call site as a review prompt rather than trying to prove the
 * type mismatch actually occurs. Tier 2 (config-injection-eligible) rather
 * than Tier 3 specifically because there IS a config that fully restores
 * the 2.4 behavior here, unlike most Family G items.
 *
 * Feeds `spark.sql.legacy.integerGroupingId`.
 */
class GroupingIdTypeDetect extends SemanticRule("GroupingIdTypeDetect") {
  override val description =
    "Flags grouping_id() call sites -- its result is Long from Spark 3.0 onward, not Int."

  private val groupingIdMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.grouping_id")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(groupingIdMatcher(_), _) =>
        RuleFinding.report(
          RuleChange(
            "GroupingIdTypeDetect",
            "grouping_id() returns Long from Spark 3.0 onward (was Int in 2.4); verify any code that type-matches, casts, or compares its result as Int.",
            "No auto-rewrite; fix the Int assumption in code, or inject spark.sql.legacy.integerGroupingId to restore the old return type.",
            t
          )
        )
    }.asPatch
  }
}
