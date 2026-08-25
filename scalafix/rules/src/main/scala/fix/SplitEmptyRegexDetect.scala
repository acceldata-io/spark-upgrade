package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector (design review Part 2 Family F): since Spark 3.4,
 * `split(str, "")` (an empty-string regex) no longer produces a trailing
 * empty string in the result array the way 2.4 did. No legacy config exists
 * for this, so it's detect-only -- narrowed to a literal empty-string second
 * argument, the only shape that's statically certain.
 *
 * (Audited whether the 3-arg `split(str, pattern, limit)` overload should
 * also be matched: verified against the real 2.4.8 jar that
 * `functions.split` has ONLY the 2-arg form there -- the 3-arg overload was
 * added in 3.0, so a 2.4.8 codebase being migrated can never contain that
 * shape in the first place. Not a reachable gap; left as exact 2-arg only.)
 */
class SplitEmptyRegexDetect extends SemanticRule("SplitEmptyRegexDetect") {
  override val description =
    "Flags split(col, \"\") with a literal empty-string regex -- Spark 3.4 changed how trailing empty strings are handled in the result array."

  private val splitFunMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.split")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(splitFunMatcher(_), List(_, Lit.String(""))) =>
        RuleFinding.report(
          RuleChange(
            "SplitEmptyRegexDetect",
            "split(col, \"\") uses an empty-string regex; Spark 3.4 no longer includes a trailing empty string in the result array the way 2.4 did.",
            "No auto-rewrite and no legacy config restores this -- review any downstream logic that depends on the old trailing-empty-element behavior.",
            t
          )
        )
    }.asPatch
  }
}
