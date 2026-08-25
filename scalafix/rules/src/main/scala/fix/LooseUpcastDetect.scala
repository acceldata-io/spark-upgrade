package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A detector (design review Part 2 Family E / Part 4):
 * `Dataset.as[T]` performs an "up-cast" that Spark 3.0 validates much more
 * strictly at analysis time (e.g. `Seq("str").toDS.as[Boolean]` now fails
 * outright instead of silently coercing). Fully verifying whether a given
 * `.as[T]` is actually unsafe needs the Dataset's real element type, which
 * isn't reliably available from the call site alone in every shape this
 * rule would need to handle -- so this is deliberately narrowed to the one
 * syntactically-certain danger case: casting to one of Scala's own atomic
 * types (`Boolean`/numeric/`String`). That's exactly the class of upcast
 * the migration guide's own example demonstrates, and it excludes the
 * overwhelmingly common, safe `.as[SomeCaseClass]` idiom used for ordinary
 * typed-row conversion, which this rule must not flag.
 *
 * Feeds `spark.sql.legacy.doLooseUpcast`, but the registry's own
 * remediation note treats a hit here as "usually a genuine type mismatch
 * worth fixing in code" rather than something to paper over with the config.
 *
 * Previously matched `.as[T]` purely syntactically (any receiver), with no
 * semantic tie to `Dataset`/`Column` at all -- despite `SemanticDocument`
 * being available. `.as[T]` on an atomic type is also the idiomatic pattern
 * for Play JSON (`(json \ "count").as[Int]`) and similar libraries, entirely
 * unrelated to Spark's upcast validation change; any codebase mixing Spark
 * with one of those would have this firing constantly on ordinary field
 * extraction. Fixed to additionally require the resolved symbol to be
 * `Dataset.as`.
 */
class LooseUpcastDetect extends SemanticRule("LooseUpcastDetect") {
  override val description =
    "Flags Dataset.as[T] upcasts to an atomic Scala type, which Spark 3.0 validates more strictly and may reject at analysis time."

  private val atomicTypes: Set[String] =
    Set("Boolean", "Byte", "Short", "Int", "Long", "Float", "Double", "String")

  private val matcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.as")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.ApplyType(Term.Select(_, matcher(_)), List(targetType))
          if atomicTypes.contains(targetType.toString) =>
        RuleFinding.report(
          RuleChange(
            "LooseUpcastDetect",
            s"Dataset.as[${targetType.toString}] casts to an atomic Scala type; Spark 3.0 validates this upcast more strictly and may now reject it at analysis time (was silently permissive in 2.4).",
            "No auto-rewrite; usually indicates a genuine type mismatch worth fixing in code rather than injecting spark.sql.legacy.doLooseUpcast.",
            t
          )
        )
    }.asPatch
  }
}
