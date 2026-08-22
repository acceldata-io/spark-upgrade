package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector (design review Part 2 Family F): since Spark 3.0,
 * `MapType` is disallowed as a map key in `map(...)`/`create_map(...)` and
 * `map_from_arrays(...)` -- analysis now fails outright instead of silently
 * accepting it. No legacy config restores this (the key type itself must
 * change), so this is detect-only. Narrowed to the unambiguous case: a key
 * position whose expression is itself a direct `map(...)`/`create_map(...)`
 * call, mirroring how `HashOnMapTypeDetect` narrows its own MapType-argument
 * detection to avoid guessing at an arbitrary expression's inferred type.
 */
class MapTypeKeyInCreateMapDetect extends SemanticRule("MapTypeKeyInCreateMapDetect") {
  override val description =
    "Flags a MapType-valued key (a nested map(...)/create_map(...) expression) in map(...)/map_from_arrays(...), which Spark 3.0 disallows at analysis time."

  private val mapFunMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.map",
    "org.apache.spark.sql.functions.create_map"
  )
  private val mapFromArraysMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.map_from_arrays")
  private val arrayFunMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.array")

  private def isMapValued(arg: Term)(implicit doc: SemanticDocument): Boolean = arg match {
    case Term.Apply(mapFunMatcher(_), _) => true
    case _ => false
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(mapFunMatcher(_), args)
          if args.zipWithIndex.exists { case (a, i) => i % 2 == 0 && isMapValued(a) } =>
        RuleFinding.report(
          RuleChange(
            "MapTypeKeyInCreateMapDetect",
            "map(...)/create_map(...) has a MapType-valued key (a nested map(...)/create_map(...) expression); Spark 3.0 rejects MapType as a map key at analysis time.",
            "No auto-rewrite and no legacy config restores this -- the key's type must change.",
            t
          )
        )
      case t @ Term.Apply(mapFromArraysMatcher(_), List(Term.Apply(arrayFunMatcher(_), keyArgs), _)) if keyArgs.exists(isMapValued) =>
        RuleFinding.report(
          RuleChange(
            "MapTypeKeyInCreateMapDetect",
            "map_from_arrays(...) has a MapType-valued key element in its keys array; Spark 3.0 rejects MapType as a map key at analysis time.",
            "No auto-rewrite and no legacy config restores this -- the key's type must change.",
            t
          )
        )
    }.asPatch
  }
}
