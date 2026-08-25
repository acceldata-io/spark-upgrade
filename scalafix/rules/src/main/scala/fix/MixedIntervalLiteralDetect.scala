package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.util.matching.Regex

/**
 * Tier 2 / Class A detector (design review Part 2 Family D / Part 4): since
 * Spark 3.2, a SQL `INTERVAL` literal can no longer mix year-month units
 * (YEAR/MONTH) with day-time units (DAY/HOUR/MINUTE/SECOND) in one literal
 * -- it must be split into two ANSI interval literals. This is a SQL-text
 * concern, not a Scala-API one (interval literals live inside embedded SQL
 * strings), so it's implemented as a regex scan over the literal argument
 * of `spark.sql(...)`/`sqlContext.sql(...)` calls -- the same call sites
 * `SqlInStringDetect` finds -- rather than a Scala AST match. Regex-based
 * SQL scanning is inherently approximate (this linter rule is fuzzy: it
 * can't parse arbitrary SQL), but the specific pattern (an INTERVAL clause
 * containing both a year-month and a day-time unit keyword) is narrow
 * enough to keep false positives low.
 *
 * Feeds `spark.sql.legacy.interval.enabled`, but the registry's own
 * remediation note prefers splitting the literal into two ANSI intervals
 * over injecting the config.
 */
class MixedIntervalLiteralDetect extends SemanticRule("MixedIntervalLiteralDetect") {
  override val description =
    "Flags a SQL INTERVAL literal mixing year-month and day-time units, which Spark 3.2+ rejects as a single literal."

  private val sqlMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.SparkSession.sql",
    "org.apache.spark.sql.SQLContext.sql"
  )

  // "INTERVAL" followed by one or more (quantity, unit) pairs, e.g.
  // `INTERVAL '1' YEAR '2' DAY` or `INTERVAL 1 YEAR 2 DAY` -- each pair is
  // consumed together so a lone `YEAR`/`DAY` mention elsewhere in the SQL
  // text (not part of an INTERVAL clause) can't spuriously extend the match.
  private val intervalClause: Regex =
    """(?is)INTERVAL(?:\s+(?:'[^']*'|\d+)\s+(?:YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)S?)+""".r
  private val yearMonthUnit: Regex = """(?i)\b(YEAR|MONTH)S?\b""".r
  private val dayTimeUnit: Regex = """(?i)\b(DAY|HOUR|MINUTE|SECOND)S?\b""".r

  private def isMixed(clause: String): Boolean =
    yearMonthUnit.findFirstIn(clause).isDefined && dayTimeUnit.findFirstIn(clause).isDefined

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      // findAllMatchIn, not findFirstMatchIn -- a query can carry more than
      // one INTERVAL clause, and only checking the first missed a mixed
      // clause that wasn't also the first one in the string.
      case Term.Apply(sqlMatcher(_), List(lit @ Lit.String(sql))) if intervalClause.findAllMatchIn(sql).exists(m => isMixed(m.matched)) =>
        RuleFinding.report(
          RuleChange(
            "MixedIntervalLiteralDetect",
            "This SQL literal appears to mix year-month (YEAR/MONTH) and day-time (DAY/HOUR/MINUTE/SECOND) units in one INTERVAL clause, which Spark 3.2+ rejects. This linter rule is fuzzy -- it pattern-matches the SQL text, it doesn't parse it.",
            "No auto-rewrite; split into two ANSI interval literals instead of injecting spark.sql.legacy.interval.enabled.",
            lit
          )
        )
    }.asPatch
  }
}
