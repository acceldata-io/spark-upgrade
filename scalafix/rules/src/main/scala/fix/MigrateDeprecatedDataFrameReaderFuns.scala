package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/**
 * `DataFrameReader.json(RDD[String])` is deprecated in favor of
 * `.json(Dataset[String])`; wraps the RDD argument with
 * `<session>.createDataset(...)(Encoders.STRING)`.
 *
 * Previously hardcoded the literal identifier `"session"` regardless of
 * what the SparkSession is actually called in the target code
 * (`Patch.addLeft(rdd, "session.createDataset(")`) -- any file naming its
 * session `spark` (the overwhelmingly common convention, used by nearly
 * every other fixture in this repo) got a reference to an undefined
 * `session` identifier spliced in, a real compile break. The rule's own
 * only fixture happened to name its variable "session" too, which is
 * exactly why this went uncaught. Fixed to derive the SparkSession
 * expression from the actual `<sessionExpr>.read.json(...)` receiver chain,
 * and to skip (not guess) when that shape isn't recognized -- e.g. the
 * reader was obtained through an intermediate `val`.
 */
class MigrateDeprecatedDataFrameReaderFuns extends SemanticRule("MigrateDeprecatedDataFrameReaderFuns") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val jsonReaderMatcher = SymbolMatcher.normalized("org.apache.spark.sql.DataFrameReader.json")
    val utils = new Utils()

    def matchOnTree(e: Tree): Patch = {
      e match {
        case ns @ Term.Apply(Term.Select(readExpr, jsonReaderMatcher(_)), List(param)) =>
          (readExpr, param) match {
            case (Term.Select(sessionExpr, Term.Name("read")), utils.rddMatcher(rdd)) =>
              val rewrite = Patch.addLeft(rdd, s"$sessionExpr.createDataset(") + Patch.addRight(rdd, ")(Encoders.STRING)") +
                utils.addImportIfNotPresent(importer"org.apache.spark.sql.Encoders")
              RuleFinding.report(
                RuleChange(
                  "MigrateDeprecatedDataFrameReaderFuns",
                  "DataFrameReader.json(RDD[String]) is deprecated.",
                  s"Wrapped the RDD[String] argument with $sessionExpr.createDataset(...)(Encoders.STRING).",
                  ns
                ),
                rewrite
              )
            case _ =>
              Patch.empty
          }
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ => elem.children.map(matchOnTree).asPatch
          }
      }
    }
    matchOnTree(doc.tree)
  }
}
