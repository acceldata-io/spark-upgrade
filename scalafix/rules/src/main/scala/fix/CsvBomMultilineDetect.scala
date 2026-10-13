package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector (design review Part 2 Family H): a CSV read under
 * `multiLine=true` with no explicit `encoding` no longer picks the charset
 * up from a UTF-16/UTF-32 byte-order mark in Spark 3.x, so such a file is
 * misread. A UTF-8 BOM is still skipped, so UTF-8 files are unaffected.
 * No legacy config restores the old detection, so this is
 * detect-only -- narrowed to the unambiguous, purely syntactic case design
 * review Part 2 Family H calls "statically detectable": a `.csv(...)`
 * read call whose receiver chain sets `multiLine` to a literal `true` but
 * never sets an explicit `encoding` option, mirroring
 * `PathOptionConflictDetect`'s same chain-walking approach to the same class
 * of problem (an `.option(...)` call elsewhere in the same fluent chain).
 */
class CsvBomMultilineDetect extends SemanticRule("CsvBomMultilineDetect") {
  override val description =
    "Flags a CSV read with multiLine=true and no explicit encoding option -- Spark 3.x misreads a UTF-16/UTF-32 file there."

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
            "csv(...) read with multiLine set to true and no explicit encoding option; Spark 3.x no longer detects a UTF-16/UTF-32 " +
              "file from its byte-order mark here, so such a file is misread. A UTF-8 BOM is still skipped; UTF-8 files are unaffected.",
            "No auto-rewrite and no legacy config restores this -- if the source files can be UTF-16/UTF-32, set " +
              ".option(\"encoding\", ...) to their charset explicitly.",
            t
          )
        )
    }.asPatch
  }
}
