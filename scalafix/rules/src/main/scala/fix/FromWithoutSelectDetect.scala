package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.util.matching.Regex

/**
 * Family K (SQL migration guide, 2.4 -> 3.0): `FROM <table>` and
 * `FROM <table> SELECT <expr>` were accepted by accident in 2.4 and below and
 * are rejected outright in 3.0. A leading `FROM` is the whole signal, which
 * makes this the highest-confidence Family K item there is.
 *
 * Implemented Scala-side rather than as a sqlfluff plugin rule -- which is the
 * opposite of the "consolidate Family K into the plugin" direction, and needs
 * the reason recorded: sqlfluff's own `sparksql` dialect CANNOT PARSE the
 * construct. Confirmed by running it (`sqlfluff parse --dialect sparksql`
 * against `FROM my_table`), which yields `unparsable: !! Expected: 'statement'`
 * and a PRS violation, not a parse tree. Every plugin rule walks parsed
 * segments, so there is nothing for one to match. A plugin rule keyed on the
 * `unparsable` segment type would fire on any construct the dialect doesn't
 * cover, not just this one.
 *
 * Detect-only. The bare form has an exact fix (`FROM t` -> `SELECT * FROM t`),
 * but the `FROM t SELECT a` form needs a clause reorder, and a rule that
 * auto-rewrites one shape while silently leaving the other is worse than one
 * that reports both.
 */
class FromWithoutSelectDetect extends SemanticRule("FromWithoutSelectDetect") {
  override val description =
    "Flags a SQL string starting with FROM (`FROM t`, `FROM t SELECT c`), which Spark 2.4 accepted by accident and 3.0 rejects."

  private val sqlMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.SparkSession.sql",
    "org.apache.spark.sql.SQLContext.sql"
  )

  // Anchored at the start of the trimmed statement. `(?i)` because SQL keywords
  // are case-insensitive; `\b` so a table or CTE actually named e.g. `fromage`
  // can't trip it.
  private val leadingFrom: Regex = """(?is)\A\s*FROM\b.*""".r

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case Term.Apply(sqlMatcher(_), List(lit @ Lit.String(sql))) if leadingFrom.findFirstIn(sql).isDefined =>
        RuleFinding.report(
          RuleChange(
            "FromWithoutSelectDetect",
            "This SQL statement starts with FROM. Spark 2.4 and below accepted `FROM t` / `FROM t SELECT c` by " +
              "accident; Spark 3.0 rejects both with a parse error.",
            "No auto-rewrite; restate as `SELECT * FROM t` (bare form) or `SELECT c FROM t` (FROM-first form).",
            lit
          )
        )
    }.asPatch
  }
}
