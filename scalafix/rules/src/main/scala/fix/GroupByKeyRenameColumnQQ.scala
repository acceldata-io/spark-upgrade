package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class GroupByKeyRenameColumnQQ
    extends SemanticRule("GroupByKeyRenameColumnQQ") {
  override val description =
    """Renaming column "value" with "key" when have Dataset.groupByKey(...).count()"""

  override val isRewrite = true

  override def fix(implicit doc: SemanticDocument): Patch = {
    val ruleId = "GroupByKeyRenameColumnQQ"
    val explanation = "Since Spark 3.0, Dataset.groupByKey(...).count() names the grouping column \"key\" instead of \"value\"."

    def renamed(t: Term, replacement: String): Patch =
      RuleFinding.report(
        RuleChange(ruleId, explanation, s"Rewrote to $replacement", t),
        Patch.replaceTree(t, replacement)
      )

    // A column reference to "value": col("value") / column("value"),
    // 'value, $"value". Never inside lit(...), whose "value" is data.
    def matchColumnRefs(t: Tree): Patch = t match {
      case Term.Apply(Term.Name("lit" | "typedLit"), _) => Patch.empty
      case q"""col("value")""" => renamed(t.asInstanceOf[Term], q"""col("key")""".toString())
      case q"""column("value")""" => renamed(t.asInstanceOf[Term], q"""column("key")""".toString())
      case q"""'value""" => renamed(t.asInstanceOf[Term], q"""'key""".toString())
      case Term.Interpolate(Term.Name("$"), List(Lit.String("value")), Nil) => renamed(t.asInstanceOf[Term], "$\"key\"")
      case _ => t.children.map(matchColumnRefs).asPatch
    }

    // An argument that names the column: a bare "value" given directly to the
    // method (select("value"), withColumnRenamed("value", ...)), or a column
    // reference anywhere inside it.
    def matchArg(t: Term): Patch = t match {
      case Lit.String("value") => renamed(t, q""""key"""".toString())
      case _ => matchColumnRefs(t)
    }

    // Only the result of groupByKey(...).count() has the renamed key column:
    // not the groupByKey lambda, not mapGroups(...) output, not other frames.
    val kvCountMatcher = SymbolMatcher.normalized("org.apache.spark.sql.KeyValueGroupedDataset.count")

    def isAfterGroupByKeyCount(t: Term): Boolean =
      t.collect { case kvCountMatcher(_) => true }.nonEmpty

    def matchOnTree(t: Tree): Patch = t match {
      case Term.Apply(fun @ Term.Select(_, _), params) if isAfterGroupByKeyCount(fun) =>
        List(params.map(matchArg).asPatch, fun.children.map(matchOnTree).asPatch).asPatch
      case _ => t.children.map(matchOnTree).asPatch
    }

    // Bit of a hack, but limit our blast radius
    if (doc.input.text.contains("groupByKey") && doc.input.text.contains("value") &&
      doc.input.text.contains("org.apache.spark.sql")) {
      matchOnTree(doc.tree)
    } else {
      Patch.empty
    }
  }
}
