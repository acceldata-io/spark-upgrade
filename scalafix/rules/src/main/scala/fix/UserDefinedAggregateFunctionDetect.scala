package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector (design review Part 2 Family C): `UserDefinedAggregateFunction`
 * is deprecated in Spark 3.0 in favor of `Aggregator` (the strongly-typed
 * `TypedImperativeAggregate`-based replacement). No mechanical rewrite is
 * attempted -- converting UDAF state/update/merge/evaluate methods into an
 * `Aggregator`'s `zero`/`reduce`/`merge`/`finish` shape is a semantic
 * redesign, not a syntactic one -- so this only flags the extends clause for
 * manual migration.
 */
class UserDefinedAggregateFunctionDetect extends SemanticRule("UserDefinedAggregateFunctionDetect") {
  override val description =
    "Flags a class extending UserDefinedAggregateFunction, which is deprecated in Spark 3.0 in favor of Aggregator."

  private val udafMatch = SymbolMatcher.normalized("org.apache.spark.sql.expressions.UserDefinedAggregateFunction")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Init(udafMatch(_), _, _) =>
        RuleFinding.report(
          RuleChange(
            "UserDefinedAggregateFunctionDetect",
            "Extends UserDefinedAggregateFunction, deprecated in Spark 3.0 in favor of the strongly-typed Aggregator API.",
            "No auto-rewrite; migrating to Aggregator (zero/reduce/merge/finish) is a semantic redesign that needs a human.",
            t
          )
        )
    }.asPatch
  }
}
