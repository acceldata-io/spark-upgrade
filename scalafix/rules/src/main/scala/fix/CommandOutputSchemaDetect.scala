package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.util.matching.Regex

/**
 * Tier 2 (SQL migration guide, 2.4 -> 3.0): `SHOW DATABASES`, `SHOW TABLES`
 * and `DESCRIBE DATABASE` name their output columns differently from 3.0
 * (`databaseName` -> `namespace`, `database` -> `namespace`,
 * `database_description_item`/`_value` -> `info_name`/`info_value`), so code
 * that reads the result by the 2.4 name fails; positional access is
 * unaffected. Port of the PySpark rule of the same id (probed on 3.5.5):
 * the command at the start of a literal `spark.sql(...)`/`sqlContext.sql(...)`
 * argument, `.stripMargin` and an `s`/`f` template's fixed start included.
 *
 * Feeds `spark.sql.legacy.keepCommandOutputSchema`, which restores 2.4's
 * names for these commands only (a baseline config).
 */
class CommandOutputSchemaDetect extends SemanticRule("CommandOutputSchemaDetect") {
  override val description =
    "Flags SHOW DATABASES/SHOW TABLES/DESCRIBE DATABASE in embedded SQL, whose output columns Spark 3.0 renamed."

  private val sqlMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.SparkSession.sql",
    "org.apache.spark.sql.SQLContext.sql"
  )

  private val command: Regex =
    """(?is)^\s*(SHOW\s+(?:DATABASES|SCHEMAS)|SHOW\s+TABLES?\s+EXTENDED|SHOW\s+TABLES|DESC(?:RIBE)?\s+(?:DATABASE|SCHEMA)(?:\s+EXTENDED)?)\b.*""".r

  private def renamed(cmd: String): String =
    if (cmd.startsWith("SHOW DATABASES") || cmd.startsWith("SHOW SCHEMAS")) "databaseName -> namespace"
    else if (cmd.startsWith("DESC")) "database_description_item/database_description_value -> info_name/info_value"
    else "database -> namespace"

  private def sqlText(arg: Term): Option[String] = arg match {
    case Lit.String(s) => Some(s)
    case Term.Select(Lit.String(s), Term.Name("stripMargin")) => Some(s.stripMargin)
    // A template's fixed start names the command: s"SHOW TABLES IN $db".
    case Term.Interpolate(Term.Name("s" | "f"), Lit.String(start) :: _, _) => Some(start)
    case _ => None
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(sqlMatcher(_), List(arg)) =>
        sqlText(arg).collect { case command(found) => found.toUpperCase.split("\\s+").mkString(" ") } match {
          case Some(cmd) =>
            RuleFinding.report(
              RuleChange(
                "CommandOutputSchemaDetect",
                s"$cmd: Spark 3.0 renamed its output columns (${renamed(cmd)}). Code that reads the result by the 2.4 " +
                  "name fails; positional access is unaffected.",
                "No auto-rewrite; read the new column name, or set spark.sql.legacy.keepCommandOutputSchema=true, " +
                  "which restores 2.4's names for these commands only.",
                t
              )
            )
          case None => Patch.empty
        }
    }.asPatch
  }
}
