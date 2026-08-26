package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Detection-only (Tier 3): flags spark.sql(...)/sqlContext.sql(...) call
 * sites whose argument is interpolated or dynamically built. A LITERAL
 * argument is deliberately NOT flagged here: `SparkSQLCallExternal` and the
 * Scala-regex Family K detectors already analyze literals directly (and
 * report their own, specific findings on the same line), so flagging the
 * call site again with a generic "review separately" message added nothing
 * but a duplicate row next to the real finding. Interpolated/dynamic SQL is
 * the opposite case -- none of those other rules can see inside it (they
 * all match a bare `Lit.String`), so this is the only signal that SQL is
 * even there.
 */
class SqlInStringDetect extends SemanticRule("SqlInStringDetect") {
  override val description =
    "Flags spark.sql(...)/sqlContext.sql(...) call sites whose SQL is interpolated or dynamically built -- the shapes the sqlfluff bridge and Scala-regex SQL rules can't see, since all of them match only a literal argument."

  override def fix(implicit doc: SemanticDocument): Patch = {
    val sqlMatcher = SymbolMatcher.normalized(
      "org.apache.spark.sql.SparkSession.sql",
      "org.apache.spark.sql.SQLContext.sql"
    )
    val ruleId = "SqlInStringDetect"

    def classify(arg: Term): Option[String] = arg match {
      case _: Lit.String       => None
      case _: Term.Interpolate => Some("interpolated")
      case _                   => Some("dynamic")
    }

    doc.tree.collect {
      case t @ Term.Apply(sqlMatcher(_), List(arg)) =>
        classify(arg) match {
          case Some(kind) =>
            RuleFinding.report(
              RuleChange(
                ruleId,
                s"In-line SQL call site classified as $kind. None of this jar's SQL rules (sqlfluff bridge, Scala-regex Family K) can inspect $kind SQL -- they all require a literal argument. Review by hand for Spark 3.x SQL-dialect/behavior changes.",
                s"No auto-rewrite; classified as $kind SQL, unchecked by any other rule.",
                t
              )
            )
          case None => Patch.empty
        }
    }.asPatch
  }
}
