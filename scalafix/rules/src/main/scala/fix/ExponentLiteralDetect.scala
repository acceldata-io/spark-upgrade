package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.util.matching.Regex

/**
 * Tier 2 / Class A detector (design review Part 2 Family E / Part 4): a
 * scientific-notation numeric literal (e.g. `1E2`) in SQL text parses as
 * `Double` from Spark 3.0 onward, where 2.4 parsed it as `Decimal`. Regex
 * scan over the literal argument of `spark.sql(...)`/`sqlContext.sql(...)`
 * calls, same call sites `SqlInStringDetect` finds -- this is a SQL-text
 * concern, not a Scala-API one.
 *
 * Feeds `spark.sql.legacy.exponentLiteralAsDecimal.enabled`.
 */
class ExponentLiteralDetect extends SemanticRule("ExponentLiteralDetect") {
  override val description =
    "Flags a scientific-notation numeric literal in SQL text, which parses as Double from Spark 3.0 onward instead of 2.4's Decimal."

  private val sqlMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.SparkSession.sql",
    "org.apache.spark.sql.SQLContext.sql"
  )

  // e.g. 1E2, 1.5e-3, 4E10 -- a digit sequence (optionally with a decimal
  // point) followed by e/E and an optionally-signed exponent.
  private val exponentLiteral: Regex = """\b\d+(?:\.\d+)?[eE][+-]?\d+\b""".r

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case Term.Apply(sqlMatcher(_), List(lit @ Lit.String(sql))) if exponentLiteral.findFirstIn(sql).isDefined =>
        RuleFinding.report(
          RuleChange(
            "ExponentLiteralDetect",
            s"""This SQL literal contains a scientific-notation number (e.g. "${exponentLiteral.findFirstIn(sql).getOrElse("")}"), which parses as Double from Spark 3.0 onward, not 2.4's Decimal.""",
            "No auto-rewrite; verify downstream precision expectations, or inject spark.sql.legacy.exponentLiteralAsDecimal.enabled to restore Decimal parsing.",
            lit
          )
        )
    }.asPatch
  }
}
