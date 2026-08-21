package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class MultiLineDatasetReadWarn extends SemanticRule("MultiLineDatasetReadWarn") {
  val matcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrameReader#option")
  override val description = "MultiLine text input dataframe warning."

  private val explanation =
    "In Spark 2.4.X and below, reading multi-line textual input with \\r\\n (windows line feed) " +
      "_might_ leave \\rs. You can get this legacy behaviour by specifying a lineSep of \"\\n\", " +
      "but for most people this was a bug. This linter rule is fuzzy."

  override def fix(implicit doc: SemanticDocument): Patch = {
    // Imperfect, maybe someone will have the string "multiline" while reading from a DataFrame but it's an ok place to start.
    if (doc.input.text.contains("'multiline'") || doc.input.text.contains("\"multiline\"")) {
      doc.tree.collect {
        case matcher(read) =>
          if (read.toString.contains("multiline")) {
            RuleFinding.report(RuleChange("MultiLineDatasetReadWarn", explanation, "No auto-rewrite; review manually.", read))
          } else {
            None.asPatch
          }
      }.asPatch
    } else {
      Patch.empty
    }
  }
}
