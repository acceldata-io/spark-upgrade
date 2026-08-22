package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A detector (design review Part 2 Family E / Part 4): a
 * negative decimal scale (`DecimalType(precision, scale)` with `scale < 0`)
 * is rejected by Spark 3.0's analyzer by default (was silently permissive in
 * 2.4). Narrowed to a literal negative Int scale argument -- the only shape
 * that's statically certain without evaluating an arbitrary expression.
 *
 * Feeds `spark.sql.legacy.allowNegativeScaleOfDecimal`.
 */
class NegativeDecimalScaleDetect extends SemanticRule("NegativeDecimalScaleDetect") {
  override val description =
    "Flags DecimalType(precision, scale) constructed with a literal negative scale, which Spark 3.0 rejects by default."

  // The bare object symbol, not `...DecimalType.apply` -- `DecimalType(38, -2)`
  // resolves its "function" position to the DecimalType object itself, not
  // the desugared `.apply` method (confirmed empirically: the resolved
  // symbol here always prints with a bare trailing `.`, SemanticDB's own
  // notation for a term/object symbol, never `apply().`).
  private val decimalTypeMatcher = SymbolMatcher.normalized("org.apache.spark.sql.types.DecimalType")

  // A negative literal (`-2`) parses as unary negation of a positive Lit
  // (`Term.ApplyUnary("-", Lit.Int(2))`), not as a single `Lit.Int(-2)` --
  // scalameta never folds the sign into the literal. Matching only
  // `Lit.Int(s) if s < 0` looks correct but never fires on source text
  // written the normal way (confirmed the hard way: `Unreported` against a
  // fixture with exactly this shape).
  private def negativeScale(t: Term): Option[Int] = t match {
    case Term.ApplyUnary(Term.Name("-"), Lit.Int(n)) => Some(-n)
    case Lit.Int(n) if n < 0 => Some(n)
    case _ => None
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(decimalTypeMatcher(_), List(_, scale)) if negativeScale(scale).isDefined =>
        val s = negativeScale(scale).get
        RuleFinding.report(
          RuleChange(
            "NegativeDecimalScaleDetect",
            s"DecimalType(..., $s) uses a negative scale, which Spark 3.0 rejects at analysis time by default.",
            "No auto-rewrite; use a non-negative scale, or inject spark.sql.legacy.allowNegativeScaleOfDecimal=true to restore 2.4's permissive behavior.",
            t
          )
        )
    }.asPatch
  }
}
