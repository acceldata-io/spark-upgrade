package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector (design review Part 2 Family G, extended): Spark 3.2
 * changed `DataFrameNaFunctions`' column name-matching semantics -- the
 * design review's own table names only `.replace(...)`, but the same
 * qualified-name resolution lives on `DataFrameNaFunctions` and covers
 * `.fill(...)` too (confirmed the practical way: a richer end-to-end fixture
 * used `.na.fill(...)`, not `.replace(...)`, for exactly this landmine, and
 * `.fill(String, Seq[String])` -- the column-name overload -- exists
 * identically in 2.4.8). No legacy config exists for this, so every call
 * site on either method is flagged for manual review rather than attempting
 * to infer whether the specific columns involved are actually affected.
 */
class NaFunctionsNameMatchDetect extends SemanticRule("NaFunctionsNameMatchDetect") {
  override val description =
    "Flags na.replace(...)/na.fill(...) call sites -- Spark 3.2 changed DataFrameNaFunctions' column name-matching semantics."

  private val naFunctionsMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.DataFrameNaFunctions.replace",
    "org.apache.spark.sql.DataFrameNaFunctions.fill"
  )

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(naFunctionsMatcher(name), _) =>
        RuleFinding.report(
          RuleChange(
            "NaFunctionsNameMatchDetect",
            s"na.${name.toString}(...) call found -- Spark 3.2 changed DataFrameNaFunctions' column name-matching semantics.",
            "No auto-rewrite and no legacy config restores this -- verify the column-name matching still selects the intended columns.",
            t
          )
        )
    }.asPatch
  }
}
