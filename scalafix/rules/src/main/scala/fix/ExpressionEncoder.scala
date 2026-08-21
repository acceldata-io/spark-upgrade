package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class ExpressionEncoder extends SemanticRule("ExpressionEncoder") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val toRowMatcher = SymbolMatcher.normalized("org/apache/spark/sql/catalyst/encoders/ExpressionEncoder#toRow().")
    val fromRowMatcher = SymbolMatcher.normalized("org/apache/spark/sql/catalyst/encoders/ExpressionEncoder#fromRow().")
    val utils = new Utils()

    def matchOnTree(e: Tree): Patch = {
      e match {
        case toRowMatcher(call) =>
          // This is sketch because were messing with the string repr but it's easier
          // since we only want to replace some of our match.
          val newCall = call.toString.replace(".toRow", ".createSerializer()")
          RuleFinding.report(
            RuleChange("ExpressionEncoder", "ExpressionEncoder.toRow was replaced by createSerializer().", s"Rewrote to $newCall", call),
            Patch.replaceTree(call, newCall)
          )
        case fromRowMatcher(call) =>
          // This is sketch because were messing with the string repr but it's easier
          // since we only want to replace some of our match.
          val newCall = call.toString.replace(".fromRow", ".createDeserializer()")
          RuleFinding.report(
            RuleChange("ExpressionEncoder", "ExpressionEncoder.fromRow was replaced by createDeserializer().", s"Rewrote to $newCall", call),
            Patch.replaceTree(call, newCall)
          )
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ => elem.children.map(matchOnTree).asPatch
          }
      }
    }
    matchOnTree(doc.tree)
  }
}
