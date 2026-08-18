package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class GroupByKeyWarn extends SemanticRule("GroupByKeyWarn") {
  val matcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.groupByKey")
  override val description = "GroupByKey Warning."

  private val explanation =
    """In Spark 2.4 and below, Dataset.groupByKey results in a grouped dataset whose key
      |attribute is wrongly named "value" if the key is non-struct type (int, string, array, etc).
      |Since Spark 3.0, the grouping attribute is named "key" instead (preserved under
      |spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue, default false).
      |This linter rule is fuzzy.""".stripMargin

  override def fix(implicit doc: SemanticDocument): Patch = {
    // Hacky.
    val grpByKey = "groupByKey"
    val funcToDS = "toDS"
    val agrFunCount = "count"
    val ruleId = "GroupByKeyWarn"

    def flagged(t: Tree): Patch =
      RuleFinding.report(
        RuleChange(ruleId, explanation, "No auto-rewrite; review every \"value\" column reference derived from this groupByKey pipeline.", t)
      )

    if (doc.input.text.contains("groupByKey") && doc.input.text.contains("value") &&
      doc.input.text.contains("org.apache.spark.sql")) {
      doc.tree.collect {
        case matcher(gbk) =>
          flagged(gbk)
        case t @ Term.Apply(
            Term.Select(
              Term.Apply(
                Term.Select(
                  Term.Apply(Term.Select(_, _ @Term.Name(fName)), _),
                  gbk @ Term.Name(name)
                ),
                _
              ),
              _ @Term.Name(oprName)
            ),
            _
          )
          if grpByKey.equals(name) && funcToDS.equals(fName) && agrFunCount
            .equals(oprName) =>
          flagged(t)
      }.asPatch
    } else {
      Patch.empty
    }
  }
}
