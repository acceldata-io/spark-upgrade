package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector (SQL migration guide, 3.1 -> 3.2): `DataFrameNaFunctions.replace`
 * stopped matching column names exactly and resolves them like SQL. Probed on
 * 2.4.8 and 3.5.5: a column the frame does not have, or a nested field, was
 * skipped silently on 2.4 and throws on 3.x. Every `replace` overload takes
 * column names, so every call is flagged but `"*"` (all columns).
 *
 * Not `fill`: it resolved names the same way on 2.4.8 already -- a missing
 * name failed there too -- so flagging it reported a change that is not one.
 * No legacy config exists.
 */
class NaFunctionsNameMatchDetect extends SemanticRule("NaFunctionsNameMatchDetect") {
  override val description =
    "Flags na.replace(...) given column names -- since Spark 3.2 a missing or nested one throws, where 2.4 skipped it."

  private val replaceMatcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrameNaFunctions.replace")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(Term.ApplyType(replaceMatcher(_), _) | replaceMatcher(_), args)
          if !args.headOption.exists { case Lit.String("*") => true; case _ => false } =>
        RuleFinding.report(
          RuleChange(
            "NaFunctionsNameMatchDetect",
            "na.replace(...) given column names. Since Spark 3.2 they resolve like SQL: a column the frame does not " +
              "have, or a nested field, throws -- 2.4 skipped it silently.",
            "No auto-rewrite and no legacy config. Check every name exists at the top level of the frame; a top-level " +
              "name with a dot needs backticks.",
            t
          )
        )
    }.asPatch
  }
}
