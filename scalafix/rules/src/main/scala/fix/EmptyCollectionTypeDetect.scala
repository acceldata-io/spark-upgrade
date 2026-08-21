package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A detector (design review Part 2 Family F / Part 4):
 * `array()`/`map()` called with no arguments infers `NullType` element(s)
 * in Spark 3.0, where 2.4 inferred `StringType`. Purely syntactic and exact:
 * the zero-argument call shape is unambiguous.
 *
 * Feeds `spark.sql.legacy.createEmptyCollectionUsingStringType`, but the
 * registry's own remediation note prefers updating downstream schema
 * expectations to NullType over injecting the config.
 */
class EmptyCollectionTypeDetect extends SemanticRule("EmptyCollectionTypeDetect") {
  override val description =
    "Flags no-argument array()/map() calls, which infer NullType element(s) in Spark 3.0 instead of 2.4's StringType."

  private val emptyCollectionFunMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.array",
    "org.apache.spark.sql.functions.map"
  )

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(emptyCollectionFunMatcher(name), Nil) =>
        RuleFinding.report(
          RuleChange(
            "EmptyCollectionTypeDetect",
            s"${name.toString}() called with no arguments infers NullType element(s) in Spark 3.0, not 2.4's StringType.",
            "No auto-rewrite; update downstream schema expectations to NullType instead of injecting spark.sql.legacy.createEmptyCollectionUsingStringType.",
            t
          )
        )
    }.asPatch
  }
}
