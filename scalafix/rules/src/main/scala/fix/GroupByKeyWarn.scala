package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/**
 * Family F (SQL migration guide, 2.4 -> 3.0): Dataset.groupByKey names its
 * grouping attribute "value" for a non-struct key in 2.4, "key" from 3.0
 * (preserved under spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue).
 * Deliberately fuzzy/Tier 3: flagging every groupByKey call regardless of
 * whether the key type is actually non-struct, since that needs type
 * inference this rule doesn't do.
 *
 * Previously this matched a bare `SymbolMatcher` directly in `doc.tree.collect`,
 * which also matches the enclosing Term.Select and Term.Apply for the same
 * call -- one real `.groupByKey(...)` was reported 2-3 times at overlapping
 * positions (the exact trap PROJECT-GUIDE.md's A.9 #2 warns about, using the
 * same `Term.Name`-narrowing fix as `IsRunningLocallyWarn`/`ShuffleWriteMetricsRenameDetect`).
 * There was also a second, fully redundant branch matching purely on the
 * identifier names "toDS"/"groupByKey"/"count" chained together with NO
 * semantic backing at all -- it would fire on any unrelated class defining
 * methods with those three names in that shape, nothing to do with Spark.
 */
class GroupByKeyWarn extends SemanticRule("GroupByKeyWarn") {
  override val description = "GroupByKey Warning."

  private val matcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.groupByKey")

  private val explanation =
    """In Spark 2.4 and below, Dataset.groupByKey results in a grouped dataset whose key
      |attribute is wrongly named "value" if the key is non-struct type (int, string, array, etc).
      |Since Spark 3.0, the grouping attribute is named "key" instead (preserved under
      |spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue, default false).
      |This linter rule is fuzzy: it flags every groupByKey call regardless of key type.""".stripMargin

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case t: Term.Name if t.value == "groupByKey" && matcher.matches(t) =>
        RuleFinding.report(
          RuleChange("GroupByKeyWarn", explanation, "No auto-rewrite; review every \"value\" column reference derived from this groupByKey pipeline.", t)
        )
    }.asPatch
}
