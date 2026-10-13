package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/**
 * `ExpressionEncoder.toRow`/`fromRow` were removed in Spark 3.0 in favour of
 * `createSerializer()`/`createDeserializer()`, which return a function object.
 * `enc.toRow(x)` becomes `enc.createSerializer()(x)` and `enc.fromRow(r)`
 * becomes `enc.createDeserializer()(r)`; an unapplied `enc.toRow` (passed as
 * a function) becomes `enc.createSerializer()`.
 *
 * Tier 1, 3.x-only output (`targetOnly`): neither method exists in 2.4.8.
 * Probed on 2.4.8 and 3.5.5 for a resolved (Int, String) encoder: the same
 * UnsafeRow bytes and the same decoded values, nulls included. One
 * difference: 2.4's `toRow` reused one row buffer per encoder, so a row kept
 * without `.copy()` was overwritten by the next call (probed: decoding the
 * first row after a second `toRow` gave the second value); each
 * `createSerializer()` has its own buffer, so the kept row stays as written.
 * Code that copies the row, or uses it before the next call, is unaffected.
 * Any other shape (infix, eta-expansion) is reported as
 * ExpressionEncoder.Manual (Tier 3).
 */
class ExpressionEncoder extends SemanticRule("ExpressionEncoder") {

  private val ruleId = "ExpressionEncoder"
  private val manualId = s"$ruleId.Manual"

  private val toRowMatcher = SymbolMatcher.normalized("org/apache/spark/sql/catalyst/encoders/ExpressionEncoder#toRow().")
  private val fromRowMatcher = SymbolMatcher.normalized("org/apache/spark/sql/catalyst/encoders/ExpressionEncoder#fromRow().")

  override def fix(implicit doc: SemanticDocument): Patch = {
    def rewrite(name: Term.Name, oldName: String, newName: String): Patch = {
      val why = s"ExpressionEncoder.$oldName was removed in Spark 3.0; use $newName(), which returns a reusable function."
      name.parent match {
        case Some(sel @ Term.Select(_, n)) if (n eq name) && !sel.parent.exists(_.isInstanceOf[Term.Eta]) =>
          RuleFinding.report(
            RuleChange(ruleId, why,
              s"Replace .$oldName with .$newName() (not applied automatically). Unlike 2.4's toRow, each createSerializer() has " +
                "its own row buffer, so a row kept without .copy() is no longer overwritten by the next call.", sel),
            Patch.replaceTree(name, s"$newName()")
          )
        case _ =>
          RuleFinding.report(
            RuleChange(manualId, why, s"Not rewritten: call it as enc.$newName()(...).", name)
          )
      }
    }
    doc.tree.collect {
      case n: Term.Name if n.value == "toRow" && toRowMatcher.matches(n) => rewrite(n, "toRow", "createSerializer")
      case n: Term.Name if n.value == "fromRow" && fromRowMatcher.matches(n) => rewrite(n, "fromRow", "createDeserializer")
    }.asPatch
  }
}
