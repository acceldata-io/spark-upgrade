package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.util.matching.Regex

/**
 * Tier 2 / Class A detector (Family K):
 * `count(tbl.*)` (a single table-qualified star inside `count(...)`) is
 * rejected by Spark 3.0's SQL parser; only bare `count(*)` is allowed.
 * Regex scan over the literal argument of
 * `spark.sql(...)`/`sqlContext.sql(...)` calls, same call sites
 * `SqlInStringDetect` finds -- this is a SQL-text concern, not a Scala-API
 * one.
 *
 * Feeds `spark.sql.legacy.allowStarWithSingleTableIdentifierInCount`, which
 * restores 2.4 exactly. `count(*)` is NOT equivalent: 2.4's `count(t.*)`
 * counts rows where every column of t is non-null (probed on rows
 * (1,null),(2,3),(null,null): count(t.*) = 1, count(*) = 3 on 2.4.8).
 */
class CountStarWithTableIdentDetect extends SemanticRule("CountStarWithTableIdentDetect") {
  override val description =
    "Flags count(tbl.*) in SQL text, which Spark 3.0 rejects; in 2.4 it counts rows where every column of tbl is non-null."

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
            "This SQL literal contains count(tbl.*) (a table-qualified star), which Spark 3.0 rejects. In 2.4 it counts the rows " +
              "where every column of tbl is non-null, not all rows, so count(*) gives a different result.",
            "No code change: spark.sql.legacy.allowStarWithSingleTableIdentifierInCount=true is injected and keeps 2.4's result. " +
              "To drop the config, rewrite to count(c1, c2, ...) listing every column of tbl -- not count(*).",
            lit
          )
        )
    }.asPatch
  }
}
