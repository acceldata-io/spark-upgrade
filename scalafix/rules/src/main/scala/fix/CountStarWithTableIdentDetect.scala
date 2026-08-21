package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.util.matching.Regex

/**
 * Tier 2 / Class A detector (design review Part 2 Family K / Part 4):
 * `count(tbl.*)` (a single table-qualified star inside `count(...)`) is
 * rejected by Spark 3.0's SQL parser; only bare `count(*)` is allowed.
 * Regex scan over the literal argument of
 * `spark.sql(...)`/`sqlContext.sql(...)` calls, same call sites
 * `SqlInStringDetect` finds -- this is a SQL-text concern, not a Scala-API
 * one.
 *
 * Feeds `spark.sql.legacy.allowStarWithSingleTableIdentifierInCount`, but
 * the registry's own remediation note prefers rewriting to `count(*)` over
 * injecting the config.
 */
class CountStarWithTableIdentDetect extends SemanticRule("CountStarWithTableIdentDetect") {
  override val description =
    "Flags count(tbl.*) in SQL text, which Spark 3.0's parser rejects -- only bare count(*) is allowed."

  private val sqlMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.SparkSession.sql",
    "org.apache.spark.sql.SQLContext.sql"
  )

  private val countTableStar: Regex = """(?i)\bcount\s*\(\s*\w+\.\*\s*\)""".r

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case Term.Apply(sqlMatcher(_), List(lit @ Lit.String(sql))) if countTableStar.findFirstIn(sql).isDefined =>
        RuleFinding.report(
          RuleChange(
            "CountStarWithTableIdentDetect",
            "This SQL literal contains count(tbl.*) (a table-qualified star), which Spark 3.0's parser rejects -- only bare count(*) is allowed.",
            "No auto-rewrite; rewrite to count(*) instead of injecting spark.sql.legacy.allowStarWithSingleTableIdentifierInCount.",
            lit
          )
        )
    }.asPatch
  }
}
