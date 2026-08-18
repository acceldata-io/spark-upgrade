package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Detection-only (Tier 3): flags every spark.sql(...)/sqlContext.sql(...)
 * call site and classifies the query argument as literal, interpolated, or
 * dynamically built. In-line SQL rewriting is a separate concern from the
 * Scala-API rules in this jar (see the sqlfluff-based `sql/` tooling) --
 * this rule's job is just to surface call sites for the Analysis report's
 * dedicated SQL-in-string section, not to rewrite anything.
 */
class SqlInStringDetect extends SemanticRule("SqlInStringDetect") {
  override val description =
    "Flags spark.sql(...)/sqlContext.sql(...) call sites and classifies the query as literal, interpolated, or dynamic."

  override def fix(implicit doc: SemanticDocument): Patch = {
    val sqlMatcher = SymbolMatcher.normalized(
      "org.apache.spark.sql.SparkSession.sql",
      "org.apache.spark.sql.SQLContext.sql"
    )
    val ruleId = "SqlInStringDetect"

    def classify(arg: Term): String = arg match {
      case _: Lit.String       => "literal"
      case _: Term.Interpolate => "interpolated"
      case _                   => "dynamic"
    }

    doc.tree.collect {
      case t @ Term.Apply(sqlMatcher(_), List(arg)) =>
        val kind = classify(arg)
        RuleFinding.report(
          RuleChange(
            ruleId,
            s"In-line SQL call site classified as $kind. Review separately for Spark 3.x SQL-dialect/behavior changes (reserved words, cast semantics, etc.) that the Scala-API rules in this jar don't cover.",
            s"No auto-rewrite; classified as $kind SQL.",
            t
          )
        )
    }.asPatch
  }
}
