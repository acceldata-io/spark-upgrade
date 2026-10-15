package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A (SQL migration guide, 3.1 -> 3.2): `saveAsTable` with a
 * `path` option creates an external table at that location, and from Spark 3.2
 * that CTAS fails when the location is a non-empty directory. 2.4 overwrote
 * it. The usual victim is a rerun: dropping an external table keeps its files,
 * so the next run finds the directory full.
 *
 * Read from 3.5.5's source: DataFrameWriter.saveAsTable -> CreateTable ->
 * CreateDataSourceTableAsSelectCommand, which calls
 * DataWritingCommand.assertEmptyRootPath only when the table does not exist
 * and the mode is ErrorIfExists. Overwrite (drops and recreates), Append and
 * Ignore never reach the check, nor does a Hive-format table
 * (CreateHiveTableAsSelectCommand) or a DataSource V2 table (Delta, Iceberg).
 * So this flags a `path` option (`.option("path", ...)`, or a "path" key of
 * `.options(Map(...))`) on the writer chain when the mode is absent (the
 * default is ErrorIfExists), an ErrorIfExists spelling, or not readable.
 * A writer chain held in a `val` in the same file is followed.
 *
 * Feeds `spark.sql.legacy.allowNonEmptyLocationInCTAS`.
 */
class SaveAsTablePathDetect extends SemanticRule("SaveAsTablePathDetect") {
  override val description =
    "Flags saveAsTable with a `path` option in ErrorIfExists (default) mode; from Spark 3.2 it fails when the location is not empty, where 2.4 overwrote it."

  private val saveAsTableMatcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrameWriter.saveAsTable")

  private val errorModes = Set("error", "errorifexists", "default")
  private val unaffectedFormats = Set("hive", "delta", "iceberg")

  /** Bodies of every `val` in this document, by name. */
  private def valBodies(implicit doc: SemanticDocument): Map[String, Term] =
    doc.tree.collect {
      case Defn.Val(_, List(Pat.Var(Term.Name(n))), _, body) => n -> body
    }.toMap

  /** The calls of the writer chain, down to `.write`, following a root that names a val once. */
  private def chain(t: Term, vals: Map[String, Term], followed: Boolean = false): List[Term.Apply] = t match {
    case Term.Select(_, Term.Name("write")) => Nil
    case app @ Term.Apply(Term.Select(q, _), _) => app :: chain(q, vals, followed)
    case Term.Select(q, _) => chain(q, vals, followed)
    case Term.Name(n) if !followed && vals.contains(n) => chain(vals(n), vals, followed = true)
    case _ => Nil
  }

  private def isPathKey(t: Tree): Boolean = t match {
    case Lit.String(k) => k.equalsIgnoreCase("path")
    case _ => false
  }

  private def isPathPair(t: Tree): Boolean = t match {
    case Term.ApplyInfix(k, Term.Name("->" | "→"), _, List(_)) => isPathKey(k)
    case Term.Tuple(List(k, _)) => isPathKey(k)
    case _ => false
  }

  private def setsPath(call: Term.Apply, vals: Map[String, Term]): Boolean = call match {
    case Term.Apply(Term.Select(_, Term.Name("option")), List(k, _)) => isPathKey(k)
    case Term.Apply(Term.Select(_, Term.Name("options")), List(arg)) =>
      (arg match {
        case Term.Name(n) => vals.get(n)
        case other => Some(other)
      }).exists {
        case Term.Apply(Term.Name("Map"), pairs) => pairs.exists(isPathPair)
        case _ => false
      }
    case _ => false
  }

  /** Some(true) for ErrorIfExists or no mode, Some(false) for another mode, None when unreadable. */
  private def errorIfExists(calls: List[Term.Apply]): Option[Boolean] =
    calls.collectFirst { case Term.Apply(Term.Select(_, Term.Name("mode")), List(arg)) => arg } match {
      case None => Some(true)
      case Some(Lit.String(m)) => Some(errorModes.contains(m.toLowerCase))
      case Some(Term.Select(_, Term.Name(m))) => Some(m == "ErrorIfExists")
      case Some(_) => None
    }

  private def format(calls: List[Term.Apply]): Option[String] =
    calls.collectFirst { case Term.Apply(Term.Select(_, Term.Name("format")), List(Lit.String(f))) => f.toLowerCase }

  override def fix(implicit doc: SemanticDocument): Patch = {
    lazy val vals = valBodies
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(recv, saveAsTableMatcher(_)), _) =>
        val calls = chain(recv, vals)
        val mode = errorIfExists(calls)
        if (!calls.exists(setsPath(_, vals)) || mode.contains(false) || format(calls).exists(unaffectedFormats)) Patch.empty
        else {
          val inMode = if (mode.isEmpty) "a mode that may be ErrorIfExists" else "ErrorIfExists mode (the default)"
          RuleFinding.report(
            RuleChange(
              "SaveAsTablePathDetect",
              s"saveAsTable with a `path` option, in $inMode. When the table does not exist but the directory holds " +
                "files -- the usual rerun, since dropping an external table keeps its files -- Spark 3.2+ fails the " +
                "write (non-empty CTAS location), where 2.4 overwrote the directory.",
              "No auto-rewrite; the tool injects spark.sql.legacy.allowNonEmptyLocationInCTAS=true to keep 2.4's " +
                "behaviour. To drop the config, write with mode(\"overwrite\") or clear the directory first.",
              t
            )
          )
        }
    }.asPatch
  }
}
