package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/**
 * Family C: `HiveContext` has been deprecated since Spark 2.0 and still compiles on 3.5.5 (with a
 * deprecation warning; `javap` on both jars); `SparkSession.builder().enableHiveSupport()` is the entry point and has
 * existed unchanged since Spark 2.0 -- verified via `javap` against the real
 * 2.4.8 AND 3.5.5 jars that `enableHiveSupport()`, `getOrCreate()`, and the
 * `.sqlContext` accessor all exist identically in both. The construction and
 * import rewrites below are exact and semantics-preserving (Spark only
 * allows one active `SparkContext` per JVM, so `getOrCreate()` reuses the
 * exact `sc` that was just constructed rather than creating a new one) --
 * promoted from Tier 3 to Tier 1 in a tier audit; there was no
 * documented reason it was Tier 3, just under-classification.
 *
 * The bare TYPE REFERENCE case (a `HiveContext`-typed val/param rewritten to
 * `SQLContext`) was initially split into a separate, lower-tier sub-rule id
 * (`MigrateHiveContext.TypeReference`) since widening a declared type can
 * break compilation elsewhere in the same file if a Hive-specific method
 * (e.g. `refreshTable`) is later called on that now-`SQLContext`-typed
 * reference. That split turned out to be cosmetic and actively misleading:
 * Tier 1 rules are run at the TOP-LEVEL rule-id granularity only (one
 * `scalafixAll <ruleId>` per Tier 1 id in `RuleRegistry`), and dotted sub-ids
 * are never invoked on their own -- so once `MigrateHiveContext` itself is
 * Tier 1, running it runs this rule's `fix()` in full, which emits (and
 * applies) the type-reference patches too,
 * regardless of what sub-id string they're reported under. A separate,
 * lower tier for that sub-id wouldn't have stopped it from being
 * auto-applied -- it would only have made the findings falsely claim it
 * still needed manual review. Merged back under the parent rule id instead;
 * the residual risk is real but narrow (this specific shape is caught by
 * a compile of the migrated code before it ever ships, the same backstop
 * every other Tier 1 rewrite in this codebase relies on) and only a genuine
 * rule SPLIT (a distinct `SemanticRule` class with its own SPI entry) could
 * actually gate it independently -- not worth the complexity for how rare
 * "the code still needs a Hive-only method after retyping" is in practice.
 *
 * Also removed the `HiveContext.getOrCreate(sc)` case that used to sit here:
 * `SymbolMatcher.normalized("org.apache.spark.sql.hive.HiveContext.getOrCreate")`
 * targeted a symbol that never existed -- confirmed via the jar listing that
 * `HiveContext` has no companion object at all in 2.4.8 (no `HiveContext$.class`),
 * unlike `SQLContext`, which does carry a real `getOrCreate(SparkContext)`.
 * That branch was dead code, the same class of bug as `OnFailureFix`'s
 * dead matcher.
 */
class MigrateHiveContext extends SemanticRule("MigrateHiveContext") {

  private val explanation = "HiveContext has been deprecated since Spark 2.0 (it still compiles on 3.5.5, with a warning); SparkSession.builder().enableHiveSupport() is the entry point."
  private val typeRefExplanation = "HiveContext has been deprecated since Spark 2.0; a HiveContext-typed reference is retyped to SQLContext (review any Hive-specific method calls on it first)."

  override def fix(implicit doc: SemanticDocument): Patch = {
    val hiveSymbolMatcher = SymbolMatcher.normalized("org.apache.spark.sql.hive.HiveContext")
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
          RuleFinding.report(RuleChange("MigrateHiveContext", typeRefExplanation, "Rewrote to SQLContext", h), Patch.replaceTree(h, "SQLContext"))
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
