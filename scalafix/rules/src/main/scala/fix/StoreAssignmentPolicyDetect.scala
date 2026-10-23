package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A (SQL migration guide, 2.4 -> 3.0): a write into an existing
 * table's schema now follows `spark.sql.storeAssignmentPolicy=ANSI`. A value
 * whose type cannot be safely cast to the column's -- string to int or date,
 * boolean to numeric -- fails analysis whatever the data
 * (INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST), and a numeric overflow fails
 * at run time (CAST_OVERFLOW_IN_TABLE_INSERT). 2.4 cast as LEGACY does: a bad
 * string became null, an overflow wrapped.
 *
 * Probed on 3.5.5 against 2.4.8: the three shapes that write into a table's
 * existing schema fail, and LEGACY gives 2.4.8's rows exactly --
 *   - `INSERT INTO|OVERWRITE [TABLE] t ...` in embedded SQL (reported by
 *     the embedded-SQL linter, from `SparkSqlExtract`'s text,
 *     so a template is read too -- as the Java recipe leaves it);
 *   - `DataFrameWriter.insertInto`;
 *   - `DataFrameWriter.saveAsTable` in append mode.
 * `saveAsTable` in any other mode, CTAS and `INSERT OVERWRITE DIRECTORY` take
 * their schema from the query and never fail, so they are not flagged.
 *
 * Whether a given write is hit depends on its column types against the table's,
 * which the rule cannot see; restoring LEGACY is what 2.4 did for every write,
 * which is why this is Tier 2. In a repo that uses Iceberg or Delta it is a
 * Tier 3 finding instead: Spark refuses LEGACY for a DataSource V2 write.
 *
 * Feeds `spark.sql.storeAssignmentPolicy`.
 */
class StoreAssignmentPolicyDetect extends SemanticRule("StoreAssignmentPolicyDetect") {
  override val description =
    "Flags a write into an existing table (insertInto, saveAsTable in append mode; a SQL INSERT is spark-migrate-cli's SQL linter's); from Spark 3.0 an unsafe type conversion fails it instead of casting."

  private val insertIntoMatcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrameWriter.insertInto")
  private val saveAsTableMatcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrameWriter.saveAsTable")

  private val Change =
    "No auto-rewrite; cast each value to its column's type in the write, or inject spark.sql.storeAssignmentPolicy=LEGACY " +
      "(not allowed for an Iceberg or Delta table)."

  private sealed trait Mode
  private case object Append extends Mode
  private case object NotAppend extends Mode
  private case object Unknown extends Mode

  /** The `.mode(...)` on the writer chain's own receiver spine, if any. */
  private def modeOf(recv: Tree): Option[Mode] = recv match {
    case Term.Apply(Term.Select(inner, Term.Name("mode")), List(arg)) =>
      Some(arg match {
        case Lit.String(m) => if (m.equalsIgnoreCase("append")) Append else NotAppend
        case Term.Select(_, Term.Name(m)) => if (m == "Append") Append else NotAppend
        case _ => Unknown
      })
    case Term.Apply(inner, _) => modeOf(inner)
    case Term.Select(inner, _) => modeOf(inner)
    case _ => None
  }

  private def finding(what: String, tree: Tree)(implicit doc: SemanticDocument): Patch =
    RuleFinding.report(
      RuleChange(
        "StoreAssignmentPolicyDetect",
        s"$what writes into an existing table's schema. From Spark 3.0 (spark.sql.storeAssignmentPolicy=ANSI) a value " +
          "that cannot be safely cast to its column's type -- string to int or date, boolean to numeric -- fails the " +
          "write at analysis, and a numeric overflow fails at run time; 2.4 cast it (a bad string became null, an " +
          "overflow wrapped).",
        Change,
        tree
      )
    )

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(_, insertIntoMatcher(_)), _) =>
        finding("insertInto", t)
      case t @ Term.Apply(Term.Select(recv, saveAsTableMatcher(_)), _) if modeOf(recv).exists(_ != NotAppend) =>
        val what = if (modeOf(recv).contains(Append)) "saveAsTable in append mode" else "saveAsTable with a mode that may be append"
        finding(what, t)
    }.asPatch
  }
}
