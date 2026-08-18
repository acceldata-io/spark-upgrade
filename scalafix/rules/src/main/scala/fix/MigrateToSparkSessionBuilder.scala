package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class MigrateToSparkSessionBuilder extends SemanticRule("MigrateToSparkSessionBuilder") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val sqlSymbolMatcher = SymbolMatcher.normalized("org.apache.spark.sql.SQLContext")
    val sqlGetOrCreateMatcher = SymbolMatcher.normalized("org.apache.spark.sql.SQLContext.getOrCreate")
    val newCreate = "SparkSession.builder.getOrCreate().sqlContext"
    val ruleId = "MigrateToSparkSessionBuilder"
    val explanation = "Direct SQLContext construction/getOrCreate is deprecated; SparkSession.builder() is the Spark 3.x entry point."

    def rewriteToBuilder(ns: Tree): Patch =
      RuleFinding.report(
        RuleChange(ruleId, explanation, s"Rewrote to $newCreate", ns),
        List(
          Patch.replaceTree(ns, newCreate),
          Patch.addGlobalImport(importer"org.apache.spark.sql.SparkSession")
        ).asPatch
      )

    def matchOnTree(e: Tree): Patch = {
      e match {
        // Rewrite the construction of a SQLContext
        case ns @ Term.New(Init(initArgs)) =>
          initArgs match {
            case (sqlSymbolMatcher(s), _, _) => rewriteToBuilder(ns)
            case _ => Patch.empty
          }
        case ns @ Term.Apply(sqlGetOrCreateMatcher(_), _) => rewriteToBuilder(ns)
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
