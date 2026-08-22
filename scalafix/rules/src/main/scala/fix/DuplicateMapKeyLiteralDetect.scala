package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector (design review Part 2 Family F): duplicate map keys throw
 * `RuntimeException` under Spark 3.0's default `spark.sql.mapKeyDedupPolicy`
 * (was silently last-write-wins in 2.4). That config is generally Class B
 * (data-dependent -- LegacyConfigRegistry's classB entry) because whether
 * duplicates occur normally depends on runtime data. But when both keys are
 * literal values written directly in a `map(...)`/`create_map(...)` call,
 * the duplication is certain at compile time, not data-dependent -- narrowed
 * to exactly that zero-false-positive case (a literal, or a `lit(...)`-
 * wrapped literal, repeated in an even/key position of the call).
 * Deliberately kept Tier 3 rather than promoted to a new Class A config
 * entry: it would share `spark.sql.mapKeyDedupPolicy` with the existing
 * data-dependent Class B entry, and the registry has no way to express
 * "sometimes Class A, sometimes Class B" for one config key.
 */
class DuplicateMapKeyLiteralDetect extends SemanticRule("DuplicateMapKeyLiteralDetect") {
  override val description =
    "Flags map(...)/create_map(...) calls with a duplicate literal key, which Spark 3.0 throws on by default instead of silently keeping the last value."

  private val mapFunMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.map",
    "org.apache.spark.sql.functions.create_map"
  )
  private val litFunMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.lit")

  private def literalKeyValue(arg: Term)(implicit doc: SemanticDocument): Option[Any] = arg match {
    case l: Lit => Some(l.value)
    case Term.Apply(litFunMatcher(_), List(l: Lit)) => Some(l.value)
    case _ => None
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(mapFunMatcher(_), args) =>
        val keyValues = args.zipWithIndex.collect { case (a, i) if i % 2 == 0 => a }.flatMap(literalKeyValue)
        val dupes = keyValues.groupBy(identity).collect { case (v, occurrences) if occurrences.size > 1 => v }.toSeq
        if (dupes.nonEmpty)
          RuleFinding.report(
            RuleChange(
              "DuplicateMapKeyLiteralDetect",
              s"map(...)/create_map(...) has duplicate literal key(s): ${dupes.mkString(", ")}. Spark 3.0 throws RuntimeException on duplicate map keys by default (spark.sql.mapKeyDedupPolicy=EXCEPTION) instead of 2.4's silent last-write-wins.",
              "No auto-rewrite; remove the duplicate key, or set spark.sql.mapKeyDedupPolicy=LAST_WIN to keep 2.4's behavior.",
              t
            )
          )
        else Patch.empty
    }.asPatch
  }
}
