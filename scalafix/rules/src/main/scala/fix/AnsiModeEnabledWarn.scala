package fix

import scalafix.v1._
import scala.meta._

case class AnsiModeEnabledWarning(tn: scala.meta.Tree) extends Diagnostic {
  override def position: Position = tn.pos

  override def message: String =
    """Since Spark 4.0, ANSI SQL mode (spark.sql.ansi.enabled) is enabled by default.
      |Operations that previously returned NULL on error - such as numeric overflow on
      |cast, division by zero, and invalid date/number parsing - may now throw exceptions.
      |Review casts and arithmetic, or set spark.sql.ansi.enabled to false to retain the
      |pre-4.0 behavior.
      |This linter rule is fuzzy.""".stripMargin
}

/**
 * Previously matched the bare syntactic shape
 * `Term.Select(Term.Name("SparkSession"), Term.Name("builder"))` -- zero
 * symbol resolution despite being a `SemanticRule` -- gated by a redundant
 * `doc.input.text.contains("SparkSession")` check that can never exclude
 * anything the inner match wouldn't already require. Two consequences: a
 * locally-defined class/object/mock literally named `SparkSession` with a
 * `.builder` member (plausible in test-mocking code) would false-positive,
 * and an aliased import (`import org.apache.spark.sql.{SparkSession => SS}`)
 * would false-negative since the receiver's rendered text is `"SS"`, not
 * `"SparkSession"`. Fixed to resolve the real companion-object method.
 */
class AnsiModeEnabledWarn extends SemanticRule("AnsiModeEnabledWarn") {
  override val description =
    "Warn that ANSI SQL mode is enabled by default starting in Spark 4.0."

  private val matcher = SymbolMatcher.normalized("org.apache.spark.sql.SparkSession.builder")

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      // Attach a single hint where the SparkSession is built to avoid flagging every cast.
      // `builder` is called both with and without `()` in real code -- match
      // the bare Term.Name (narrowed, per the SymbolMatcher multi-report
      // trap) so both call styles are caught, rather than requiring a
      // Term.Apply that a paren-less call site never has.
      case t: Term.Name if t.value == "builder" && matcher.matches(t) => Patch.lint(AnsiModeEnabledWarning(t))
    }.asPatch
}
