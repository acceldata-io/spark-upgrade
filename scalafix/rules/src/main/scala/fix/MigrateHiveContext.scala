package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class MigrateHiveContext extends SemanticRule("MigrateHiveContext") {

  private val explanation = "HiveContext is removed; SparkSession.builder().enableHiveSupport() is the Spark 3.x entry point."

  override def fix(implicit doc: SemanticDocument): Patch = {
    val hiveSymbolMatcher = SymbolMatcher.normalized("org.apache.spark.sql.hive.HiveContext")
    val hiveGetOrCreateMatcher = SymbolMatcher.normalized("org.apache.spark.sql.hive.HiveContext.getOrCreate")
    val newCreateHive = "SparkSession.builder.enableHiveSupport().getOrCreate().sqlContext"
    val utils = new Utils()
    def matchOnTree(e: Tree): Patch = {
      e match {
        // Rewrite the construction of a HiveContext
        case ns @ Term.New(Init(initArgs)) =>
          initArgs match {
            case (hiveSymbolMatcher(_), _, _) =>
              val rewrite = List(
                Patch.replaceTree(
                  ns,
                  newCreateHive),
                  // TODO Add SparkSession import if missing -- addGlobalImport is broken
                  // Patch.addGlobalImport(importer"org.apache.spark.sql.SparkSession")
                utils.addImportIfNotPresent(importer"org.apache.spark.sql.SparkSession")
              ).asPatch
              RuleFinding.report(RuleChange("MigrateHiveContext", explanation, s"Rewrote to $newCreateHive", ns), rewrite)
            // A non-Hive `new Foo(...)` used to short-circuit to Patch.empty
            // here instead of falling through to the generic recursive case
            // below -- so a nested `new HiveContext(...)` inside another
            // constructor's arguments (e.g. `new Wrapper(new HiveContext(sc))`)
            // was never visited at all. Recurse into this node's own children
            // instead of dropping them.
            case _ => ns.children.map(matchOnTree).asPatch
          }
        case ns @ Term.Apply(hiveGetOrCreateMatcher(_), _) =>
          val rewrite = List(
            Patch.replaceTree(
              ns,
              newCreateHive),
              // TODO Add SparkSession import if missing -- addGlobalImport is broken
              // Patch.addGlobalImport(importer"org.apache.spark.sql.SparkSession")
              utils.addImportIfNotPresent(importer"org.apache.spark.sql.SparkSession")
          ).asPatch
          RuleFinding.report(RuleChange("MigrateHiveContext", explanation, s"Rewrote to $newCreateHive", ns), rewrite)

        // HiveContext type name rewrite to SQLContext
        // There should be a way to combine these two rules right?
        // Ideally we could rewrite the import to SqlContext symbol.
        case imp @ Import(List(
          Importer(Term.Select(Term.Select(Term.Select(
            Term.Select(Term.Name("org"), Term.Name("apache")),
            Term.Name("spark")),
            Term.Name("sql")),
            Term.Name("hive")),
            List(hiveImports)))) =>
          // Remove HiveContext it's deprecated
          hiveImports.collect {
            case i @ Importee.Name(Name("HiveContext")) =>
              val rewrite = List(
                Patch.removeImportee(i),
                utils.addImportIfNotPresent(importer"org.apache.spark.sql.SQLContext")
                // TODO add SQLContext import if missing -- addGlobalImport is broken
              ).asPatch
              RuleFinding.report(RuleChange("MigrateHiveContext", explanation, "Removed the HiveContext import; added SQLContext.", imp), rewrite)
            case i @ Importee.Rename(Name("HiveContext"), _) =>
              val rewrite = List(
                Patch.removeImportee(i),
                utils.addImportIfNotPresent(importer"org.apache.spark.sql.SQLContext")
                // TODO add SQLContext import if missing -- addGlobalImport is broken
              ).asPatch
              RuleFinding.report(RuleChange("MigrateHiveContext", explanation, "Removed the HiveContext import; added SQLContext.", imp), rewrite)
            case _ => Patch.empty
          }.asPatch
        case hiveSymbolMatcher(h) =>
          RuleFinding.report(RuleChange("MigrateHiveContext", explanation, "Rewrote to SQLContext", h), Patch.replaceTree(h, "SQLContext"))
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
