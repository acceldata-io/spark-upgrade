package fix

import fix.support.{ColumnNames, RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Family F: since Spark 3.4, `split(str, "")` (an
 * empty-string regex) no longer ends the result with an empty string the way
 * 2.4 did ("abc" -> [a, b, c], where 2.4 gave [a, b, c, ""]). No config
 * restores it, but the regex "(?=)" -- a zero-width match at every position,
 * which is what 2.4's Java String.split did with "" -- gives 2.4's result on
 * 3.5.5 (and on 2.4.8 itself). Probed on both for "abc", "", null, one
 * character, separators, spaces, accented letters and a supplementary
 * character (split into its two halves on both, as 2.4 did).
 *
 * The rewrite changes the auto-generated column name (`split(s, )` on 2.4,
 * `split(s, (?=), -1)` on 3.5.5; unchanged, 3.5.5 names it `split(s, , -1)`),
 * so it is applied only where that name is discarded: aliased, withColumn's
 * value, or a filter/where/orderBy/sort/join argument. Any other call is
 * reported as SplitEmptyRegexDetect.Manual (Tier 3) and left as written.
 *
 * Only the 2-arg form with a literal "" is matched: `functions.split` has no
 * 3-arg overload in 2.4.8 (javap), so a 2.4 codebase cannot contain one.
 */
class SplitEmptyRegexDetect extends SemanticRule("SplitEmptyRegexDetect") {
  override val description =
    "Rewrites split(col, \"\") to split(col, \"(?=)\"), which keeps Spark 2.4's trailing empty string on Spark 3.4+."

  private val ruleId = "SplitEmptyRegexDetect"

  private val splitFunMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.split")

  private val explanation =
    "split(col, \"\") splits on an empty regex. Since Spark 3.4 the result has no trailing empty string " +
      "(\"abc\" -> [a, b, c], where 2.4 gave [a, b, c, \"\"]); no config restores it."

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(splitFunMatcher(_), List(_, pattern @ Lit.String(""))) =>
        if (ColumnNames.nameDiscarded(t))
          RuleFinding.report(
            RuleChange(ruleId, explanation, "Rewrote the regex to \"(?=)\", which gives 2.4's result on Spark 3.x too.", t),
            Patch.replaceTree(pattern, "\"(?=)\"")
          )
        else
          RuleFinding.report(
            RuleChange(
              s"$ruleId.Manual",
              explanation,
              "Not rewritten: the result may become a named output column, and the rewrite changes that column's " +
                "auto-generated name. To keep 2.4's result use split(col, \"(?=)\") and alias it " +
                ".as(\"<the name 2.4 gave it>\") if the column name matters.",
              t
            )
          )
    }.asPatch
  }
}
