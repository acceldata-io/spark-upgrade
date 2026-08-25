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

    def matchOnTerm(t: Term): Patch = {
      val p = t match {
        case q""""value"""" => renamed(t, q""""key"""".toString())
        case q"""'value"""  => renamed(t, q"""'key""".toString())
        case q"""col("value")""" =>
          renamed(t, q"""col("key")""".toString())
        case q"""col("value").as""" =>
          renamed(t, q"""col("key").as""".toString())
        case q"""col("value").alias""" =>
          renamed(t, q"""col("key").alias""".toString())
        case q"""upper(col("value"))""" =>
          renamed(t, q"""upper(col("key"))""".toString())
        case q"""upper(col('value))""" =>
          renamed(t, q"""upper(col('key))""".toString())
        case _ if ! t.children.isEmpty =>
          t.children.map {
            case e: scala.meta.Term => matchOnTerm(e)
            case _ => Patch.empty
          }.asPatch
        case _ => Patch.empty
      }
      p
    }

    // Previously ANDed a bare `q"""groupByKey"""` name check (matches ANY
    // identifier named "groupByKey" anywhere in the chain, no symbol
    // resolution) against a loose "isDataset" check that itself accepted a
    // bare mention of the `Dataset`/`DataFrame` TYPE NAME anywhere in the
    // chain (plus a dead, typo'd `DataFame` pattern that could never match).
    // Together those are satisfied by any unrelated class exposing a
    // same-named `groupByKey` method somewhere near an unrelated
    // Dataset/DataFrame type mention -- with isRewrite = true, that silently
    // rewrites an unrelated "value" literal to "key". `dsGBKmatcher` was
    // already declared (it's what actually verified real occurrences below)
    // but was only ever used as one alternative inside the loose OR list
    // instead of being the deciding check.
    val dsGBKmatcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.groupByKey")

    def isDSGroupByKey(t: Term): Boolean =
      t.collect { case dsGBKmatcher(_) => true }.nonEmpty

    def matchOnTree(t: Tree): Patch = {
      t match {
        case _ @Term.Apply(tr, params) if (isDSGroupByKey(tr)) => {
          val patch = List(
            params.map(matchOnTerm).asPatch,
            params.map(matchOnTree).asPatch,
            tr.children.map(matchOnTree).asPatch
          ).asPatch
          patch
        }
        case elem @ _ => {
          elem.children match {
            case Nil => Patch.empty
            case _ => {
              elem.children.map(matchOnTree).asPatch
            }
          }
        }
      }
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
