package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A detector (design review Part 2 Family F / Part 4):
 * `hash()`/`xxhash64()` on a MapType column throws in Spark 3.0 (was
 * silently permissive before). Reliably resolving an arbitrary argument's
 * type back to MapType isn't always possible from the call site alone, so
 * this is narrowed to the unambiguous, zero-false-positive case: the
 * argument is itself a direct `map(...)`/`create_map(...)` call --
 * whatever else it's applied to is left alone rather than guessed at.
 *
 * Feeds `spark.sql.legacy.allowHashOnMapType`, but the registry's own
 * remediation note treats a hit as usually an unintentional hash rather
 * than something to preserve via the config.
 */
class HashOnMapTypeDetect extends SemanticRule("HashOnMapTypeDetect") {
  override val description =
    "Flags hash()/xxhash64() applied directly to a map(...)/create_map(...) result, which Spark 3.0 rejects instead of hashing."

  private val hashFunMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.hash",
    "org.apache.spark.sql.functions.xxhash64"
  )
  private val mapFunMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.map",
    "org.apache.spark.sql.functions.create_map"
  )

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(hashFunMatcher(_), args) if args.exists {
            case Term.Apply(mapFunMatcher(_), _) => true
            case _ => false
          } =>
        RuleFinding.report(
          RuleChange(
            "HashOnMapTypeDetect",
            "hash()/xxhash64() applied to a map(...)/create_map(...) result; Spark 3.0 throws on MapType input instead of hashing it.",
            "No auto-rewrite; usually an unintentional hash of a map -- review before injecting spark.sql.legacy.allowHashOnMapType.",
            t
          )
        )
    }.asPatch
  }
}
