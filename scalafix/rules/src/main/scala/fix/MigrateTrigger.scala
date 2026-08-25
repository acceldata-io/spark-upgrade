package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/**
 * `org.apache.spark.sql.streaming.ProcessingTime` (the top-level object) was
 * removed in Spark 3.0 in favor of `Trigger.ProcessingTime(...)` -- a
 * same-named but distinct static forwarder owned by `Trigger`, not by
 * `ProcessingTime` itself, so the two are different symbols and a semantic
 * matcher targeting the removed object's own symbol does not collide with
 * the replacement API.
 *
 * The two gates below (`e.toString.contains("ProcessingTime")` and the
 * outer `doc.input.text.contains("org.apache.spark.sql.streaming")`) were
 * added to "deal with spurious matches", but neither is a semantic check:
 * the first tests the MATCHED node's own rendered text, which is always
 * `"ProcessingTime"` regardless of which real occurrence matched, so it can
 * never exclude anything; the second requires that literal package string
 * to appear verbatim in the file, so an aliased import
 * (`import org.apache.spark.sql.streaming.{ProcessingTime => PT}`, still
 * resolving to the exact same removed symbol) is silently missed. Both
 * removed -- the underlying `SymbolMatcher` was already precise.
 */
class MigrateTrigger extends SemanticRule("MigrateTrigger") {
  override val description =
    """Migrate Trigger."""
  override val isRewrite = true

  private val triggerMatcher = SymbolMatcher.normalized("org.apache.spark.sql.streaming.ProcessingTime")

  override def fix(implicit doc: SemanticDocument): Patch = {
    val utils = new Utils()
    def matchOnTree(e: Tree): Patch = {
      e match {
        case triggerMatcher(t) =>
          RuleFinding.report(
            RuleChange(
              "MigrateTrigger",
              "org.apache.spark.sql.streaming.ProcessingTime was removed in favor of Trigger.ProcessingTime.",
              "Added the org.apache.spark.sql.streaming.Trigger._ import.",
              t
            ),
            utils.addImportIfNotPresent(importer"org.apache.spark.sql.streaming.Trigger._")
          )
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
