package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/**
 * Previously matched a bare `DataFrameReader#option` `SymbolMatcher` inside
 * `doc.tree.collect`, gated by `read.toString.contains("multiline")` -- a
 * text search over the *matched node's own rendered source* (which,
 * because of the SymbolMatcher-multi-report trap, could itself be the bare
 * `Term.Name`, the enclosing `Term.Select`, or the enclosing `Term.Apply` --
 * whichever text happened to contain the substring). That's both too broad
 * (`.option("header", multilineFlag)` would match on an unrelated argument
 * name) and, worse, too narrow in the one case that matters: Spark's
 * documented option key is `multiLine` (capital L) -- a lowercase-only
 * substring check never matches the spelling virtually all real code uses.
 * (Spark's own `CaseInsensitiveMap` treats option keys case-insensitively at
 * runtime, so `multiLine`/`multiline`/`MULTILINE` are all equivalent -- the
 * check should be too.) There was also an outer `doc.input.text.contains`
 * gate requiring a quoted `'multiline'`/`"multiline"` literal anywhere in
 * the file, same lowercase-only bug, and redundant with a properly-scoped
 * inner check regardless.
 *
 * Fixed to match the `.option(key, value)` call directly and compare the key
 * literal case-insensitively, reporting at the call site itself.
 */
class MultiLineDatasetReadWarn extends SemanticRule("MultiLineDatasetReadWarn") {
  private val matcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrameReader.option")
  override val description = "MultiLine text input dataframe warning."

  private val explanation =
    "In Spark 2.4.X and below, reading multi-line textual input with \\r\\n (windows line feed) " +
      "_might_ leave \\rs. You can get this legacy behaviour by specifying a lineSep of \"\\n\", " +
      "but for most people this was a bug. This linter rule is fuzzy."

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case Term.Apply(Term.Select(_, matcher(_)), List(lit @ Lit.String(key), _)) if key.equalsIgnoreCase("multiLine") =>
        RuleFinding.report(RuleChange("MultiLineDatasetReadWarn", explanation, "No auto-rewrite; review manually.", lit))
    }.asPatch
}
