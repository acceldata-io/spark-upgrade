package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/**
 * Family F (SQL migration guide, 2.4 -> 3.0): KeyValueGroupedDataset.count(),
 * .agg(...) and .reduceGroups(...) -- the calls on a groupByKey result that
 * output the grouping key as a column -- name it "value" for a non-struct key
 * in 2.4 and "key" from 3.0. Tier 2: spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue
 * restores "value" exactly (probed on 2.4.8 and 3.5.5; struct keys are
 * unaffected either way). mapGroups/flatMapGroups/keys do not output that
 * column, so they are not flagged; mapValues itself is not either, but a
 * count/agg/reduceGroups after it is.
 *
 * Matches the method's Term.Name only: a bare SymbolMatcher in
 * `doc.tree.collect` also matches the enclosing Term.Select and Term.Apply,
 * reporting one call two or three times.
 */
class GroupByKeyWarn extends SemanticRule("GroupByKeyWarn") {
  override val description =
    "Flags count()/agg(...)/reduceGroups(...) on a groupByKey result, whose non-struct grouping column is named 'key' from Spark 3.0 instead of 'value'."

  private val matcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.KeyValueGroupedDataset.count",
    "org.apache.spark.sql.KeyValueGroupedDataset.agg",
    "org.apache.spark.sql.KeyValueGroupedDataset.reduceGroups"
  )

  private val methods = Set("count", "agg", "reduceGroups")

  private val explanation =
    """In Spark 2.4, count()/agg(...)/reduceGroups(...) on the result of Dataset.groupByKey names the grouping
      |column "value" when the key is not a struct (int, string, array, etc.). Since Spark 3.0
      |it is named "key".""".stripMargin

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case t: Term.Name if methods(t.value) && matcher.matches(t) =>
        RuleFinding.report(
          RuleChange(
            "GroupByKeyWarn",
            explanation,
            "No code change: spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue=true is injected, which keeps 2.4's \"value\" column name.",
            t
          )
        )
    }.asPatch
}
