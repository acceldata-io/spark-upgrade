package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class MigrateTrigger extends SemanticRule("MigrateTrigger") {
  override val description =
    """Migrate Trigger."""
  override val isRewrite = true

  val triggerMatcher = SymbolMatcher.normalized("org.apache.spark.sql.streaming.ProcessingTime")

  override def fix(implicit doc: SemanticDocument): Patch = {
    val utils = new Utils()
    def matchOnTree(e: Tree): Patch = {
      e match {
        // Trigger match seems to be matching too widly sometimes?
        case triggerMatcher(e) =>
          if (e.toString.contains("ProcessingTime")) {
            RuleFinding.report(
              RuleChange(
                "MigrateTrigger",
                "org.apache.spark.sql.streaming.ProcessingTime was removed in favor of Trigger.ProcessingTime.",
                "Added the org.apache.spark.sql.streaming.Trigger._ import.",
                e
              ),
              utils.addImportIfNotPresent(importer"org.apache.spark.sql.streaming.Trigger._")
            )
          } else {
            None.asPatch
          }
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ => elem.children.map(matchOnTree).asPatch
          }
      }
    }
    // Deal with the spurious matches by only running on files that importing streaming.
    if (doc.input.text.contains("org.apache.spark.sql.streaming")) {
      matchOnTree(doc.tree)
    } else {
      None.asPatch
    }
  }
}
