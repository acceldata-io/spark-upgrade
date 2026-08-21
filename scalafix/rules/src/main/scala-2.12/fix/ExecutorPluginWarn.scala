package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class ExecutorPluginWarn extends SemanticRule("ExecutorPluginWarn") {
  // See https://spark.apache.org/docs/3.0.0/core-migration-guide.html +
  // + new docs at:
  // https://spark.apache.org/docs/3.2.1/api/java/index.html?org/apache/spark/api/plugin/SparkPlugin.html
  // https://spark.apache.org/docs/3.2.1/api/java/org/apache/spark/api/plugin/ExecutorPlugin.html

  val matcher = SymbolMatcher.normalized("org.apache.spark.ExecutorPlugin")

  private val explanation =
    "Executor Plugin is dropped in 3.0+, see " +
      "https://spark.apache.org/docs/3.0.0/core-migration-guide.html " +
      " https://spark.apache.org/docs/3.2.1/api/java/index.html?org/apache/spark/api/plugin/SparkPlugin.html"

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case matcher(s) =>
        RuleFinding.report(RuleChange("ExecutorPluginWarn", explanation, "No auto-rewrite; migrate to org.apache.spark.api.plugin.SparkPlugin manually.", s))
    }.asPatch
  }
}
