package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.util.matching.Regex

/**
 * Family K (SQL migration guide, 2.4 -> 3.0): a bare `FROM <table>` was
 * accepted by accident in 2.4 and below and is rejected outright in 3.0. Hive's
 * FROM-first forms, `FROM t SELECT c` and `FROM t INSERT ...`, still parse and
 * run on 3.5.5 (probed: `FROM t SELECT id` returns 1), so only a
 * leading `FROM` with no SELECT after it is flagged -- the same test to apply to
 * embedded SQL for Java and PySpark.
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
 * Tier 1: the statement becomes `SELECT * FROM ...`. Probed on
 * 2.4.8 and 3.5.5, the columns and rows match for `FROM t`, with WHERE,
 * ORDER BY/LIMIT, DISTRIBUTE BY, a JOIN, LATERAL VIEW, a table alias and a
 * subquery. Not for GROUP BY (2.4 returned zero columns there; `SELECT *` fails
 * on 3.5.5), HAVING, WINDOW, or a `;` (2.4.8 rejected the statement anyway):
 * those are reported as FromWithoutSelectDetect.Manual (Tier 3).
 */
class FromWithoutSelectDetect extends SemanticRule("FromWithoutSelectDetect") {
  override val description =
    "Rewrites a SQL string that is a bare FROM (`FROM t`, no SELECT), which Spark 2.4 accepted by accident and 3.0 rejects, to SELECT * FROM t."

  private val ruleId = "FromWithoutSelectDetect"
  private val manualId = s"$ruleId.Manual"

  private val sqlMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.SparkSession.sql",
    "org.apache.spark.sql.SQLContext.sql"
  )

  // Anchored at the start of the trimmed statement. `(?i)` because SQL keywords
  // are case-insensitive; `\b` so a table or CTE actually named e.g. `fromage`
  // can't trip it. The lookahead leaves Hive's FROM-first forms alone: each has a SELECT.
  private val leadingFrom: Regex = FromWithoutSelectDetect.leadingFrom
  // Clauses whose meaning without a SELECT is not SELECT *.
  private val notStar: Regex = """(?is).*(\bGROUP\s+BY\b|\bHAVING\b|\bWINDOW\b|;).*""".r
  // The FROM keyword in the literal's source text: after the opening quotes and any
  // whitespace, written raw or as an escape (\n, \t, \r).
  private val fromInSource: Regex = """\A(\"\"\"|\")(\s|\\[ntr])*(?i)from\b""".r

  private val explanation =
    "This SQL statement is a bare FROM with no SELECT. Spark 2.4 and below accepted `FROM t` by accident (as SELECT *); " +
      "Spark 3.0 rejects it with a parse error."

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case Term.Apply(sqlMatcher(_), List(lit @ Lit.String(sql))) if leadingFrom.findFirstIn(sql).isDefined =>
        val source = lit.syntax
        fromInSource.findFirstMatchIn(source) match {
          case Some(m) if notStar.findFirstIn(sql).isEmpty =>
            val at = m.end - "from".length
            val rewritten = source.substring(0, at) + "SELECT * " + source.substring(at)
            RuleFinding.report(
              RuleChange(ruleId, explanation, s"Rewrote to $rewritten, which is what 2.4 ran.", lit),
              Patch.replaceTree(lit, rewritten)
            )
          case _ =>
            RuleFinding.report(
              RuleChange(
                manualId,
                explanation,
                "Not rewritten: with GROUP BY, HAVING or WINDOW a bare FROM is not the same as SELECT * (2.4 returned no " +
                  "columns for a FROM ... GROUP BY), and a statement with `;` already failed on 2.4. Add the SELECT list you mean.",
                lit
              )
            )
        }
    }.asPatch
  }
}

object FromWithoutSelectDetect {
  /** A bare FROM: a leading `FROM` with no SELECT after it. Shared with
   * SparkSQLCallExternal, which leaves such a literal to this rule rather than
   * also reporting it as unparseable. */
  val leadingFrom: Regex = """(?is)\A\s*FROM\b(?!.*\bSELECT\b).*""".r
}
