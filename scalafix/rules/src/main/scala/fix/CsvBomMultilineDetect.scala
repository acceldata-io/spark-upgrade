package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector (design review Part 2 Family H): Spark 3.0 removed
 * automatic BOM (byte-order-mark) detection for CSV -- a UTF-8-with-BOM file
 * read under `multiLine=true` now gets the BOM bytes folded into the first
 * column's value instead of stripped, unless the read explicitly names its
 * `encoding`. No legacy config restores the old auto-detection, so this is
 * detect-only -- narrowed to the unambiguous, purely syntactic case design
 * review Part 2 Family H calls "statically detectable": a `.csv(...)`
 * read call whose receiver chain sets `multiLine` to a literal `true` but
 * never sets an explicit `encoding` option, mirroring
 * `PathOptionConflictDetect`'s same chain-walking approach to the same class
 * of problem (an `.option(...)` call elsewhere in the same fluent chain).
 */
class CsvBomMultilineDetect extends SemanticRule("CsvBomMultilineDetect") {
  override val description =
    "Flags a CSV read with multiLine=true and no explicit encoding option -- Spark 3.0 removed automatic BOM detection for CSV."

  private def hasOption(recv: Tree, key: String, literalValue: Option[String]): Boolean =
    recv.collect {
      case Term.Apply(Term.Select(_, Term.Name("option")), List(Lit.String(k), v)) if k == key =>
        literalValue.forall(lv => v match {
          case Lit.String(s) => s == lv
          case Lit.Boolean(b) => b.toString == lv
          case _ => false
        })
    }.contains(true)

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(recv, Term.Name("csv")), args)
          if args.nonEmpty && hasOption(recv, "multiLine", Some("true")) && !hasOption(recv, "encoding", None) =>
        RuleFinding.report(
          RuleChange(
            "CsvBomMultilineDetect",
            "csv(...) read with multiLine set to true and no explicit encoding option; Spark 3.0 removed automatic BOM detection, so a UTF-8-with-BOM file's BOM bytes now land in the first column's value instead of being stripped.",
            "No auto-rewrite and no legacy config restores this -- set .option(\"encoding\", \"UTF-8\") explicitly (or strip the BOM upstream) if the source files may have one.",
            t
          )
        )
    }.asPatch
  }
}
