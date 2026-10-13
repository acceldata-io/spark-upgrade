package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.util.matching.Regex

/**
 * Family K (SQL migration guide, 2.4 -> 3.0): a bare `FROM <table>` was
 * accepted by accident in 2.4 and below and is rejected outright in 3.0. Hive's
 * FROM-first forms, `FROM t SELECT c` and `FROM t INSERT ...`, still parse and
 * run on 3.5.5 (probed 2026-10-13: `FROM t SELECT id` returns 1), so only a
 * leading `FROM` with no SELECT after it is flagged -- the same test the CLI's
 * SqlLiteralLinter applies for Java and PySpark.
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
 * Detect-only. The bare form has an exact fix (`FROM t` -> `SELECT * FROM t`);
 * promoting it is TRIAGE-PLAN §3.1 in spark-migrate-cli.
 */
class FromWithoutSelectDetect extends SemanticRule("FromWithoutSelectDetect") {
  override val description =
    "Flags a SQL string that is a bare FROM (`FROM t`, no SELECT), which Spark 2.4 accepted by accident and 3.0 rejects."

  private val sqlMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.SparkSession.sql",
    "org.apache.spark.sql.SQLContext.sql"
  )

  // Anchored at the start of the trimmed statement. `(?i)` because SQL keywords
  // are case-insensitive; `\b` so a table or CTE actually named e.g. `fromage`
  // can't trip it. The lookahead leaves Hive's FROM-first forms alone: each has a SELECT.
  private val leadingFrom: Regex = """(?is)\A\s*FROM\b(?!.*\bSELECT\b).*""".r

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case Term.Apply(sqlMatcher(_), List(lit @ Lit.String(sql))) if leadingFrom.findFirstIn(sql).isDefined =>
        RuleFinding.report(
          RuleChange(
            "FromWithoutSelectDetect",
            "This SQL statement is a bare FROM with no SELECT. Spark 2.4 and below accepted `FROM t` by accident; " +
              "Spark 3.0 rejects it with a parse error.",
            "No auto-rewrite; restate it as `SELECT * FROM t`.",
            lit
          )
        )
    }.asPatch
  }
}
