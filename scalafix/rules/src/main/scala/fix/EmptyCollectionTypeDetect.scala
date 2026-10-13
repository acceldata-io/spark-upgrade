package fix

import fix.support.{ColumnNames, RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * `array()`/`map()` called with no arguments infers `NullType` element(s) in
 * Spark 3.0 (`array<void>`), where 2.4 inferred `StringType`.
 *
 * Tier 1 since 2026-10-13, where the change is exact: the call becomes
 * `array().cast(ArrayType(StringType, false))` /
 * `map().cast(MapType(StringType, StringType, false))`. Probed on 2.4.8 and
 * 3.5.5: the full schema, nullability flags included, matches 2.4's for a
 * selected, aliased, withColumn, coalesce, when/otherwise and concat use, and
 * the rows and the Parquet schema written match too; on 2.4.8 the cast is a
 * no-op. The DataType form matters: `.cast("array<string>")` gives
 * `containsNull = true` where 2.4 had false.
 *
 * The cast changes the auto-generated column name (`array()` becomes
 * `CAST(array() AS ARRAY<STRING>)`), so the rewrite fires only where that name
 * is discarded (`ColumnNames.nameDiscarded`). Any other call is reported as
 * EmptyCollectionTypeDetect.Config (Tier 2), which feeds
 * spark.sql.legacy.createEmptyCollectionUsingStringType, also exact. A call
 * that is already the receiver of `.cast(...)` has its type set explicitly and
 * is not flagged (this is also what keeps the rule from re-firing on its own
 * output).
 */
class EmptyCollectionTypeDetect extends SemanticRule("EmptyCollectionTypeDetect") {
  override val description =
    "Rewrites no-argument array()/map() calls, which infer NullType element(s) in Spark 3.0, to 2.4's string-typed collection."

  private val ruleId = "EmptyCollectionTypeDetect"
  private val configId = s"$ruleId.Config"

  private val emptyCollectionFunMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.array",
    "org.apache.spark.sql.functions.map"
  )

  private def isCastReceiver(t: Term): Boolean = t.parent.exists {
    case Term.Select(qual, Term.Name("cast")) => qual eq t
    case _ => false
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    val utils = new Utils()
    doc.tree.collect {
      case t @ Term.Apply(emptyCollectionFunMatcher(name), Nil) if !isCastReceiver(t) =>
        val isArray = name.symbol.displayName == "array"
        val castTo = if (isArray) "ArrayType(StringType, false)" else "MapType(StringType, StringType, false)"
        val replacement = s"$t.cast($castTo)"
        val why =
          s"${name.symbol.displayName}() with no arguments infers NullType element(s) in Spark 3.0 " +
            s"(${if (isArray) "array<void>" else "map<void,void>"}), where 2.4 inferred " +
            s"${if (isArray) "array<string>" else "map<string,string>"}."
        if (ColumnNames.nameDiscarded(t)) {
          val imports = List(
            if (isArray) importer"org.apache.spark.sql.types.ArrayType" else importer"org.apache.spark.sql.types.MapType",
            importer"org.apache.spark.sql.types.StringType"
          ).map(utils.addImportIfNotPresent).asPatch
          RuleFinding.report(
            RuleChange(ruleId, why, s"Rewrote to $replacement, 2.4's exact type (from org.apache.spark.sql.types).", t),
            Patch.replaceTree(t, replacement) + imports
          )
        } else {
          RuleFinding.report(
            RuleChange(
              configId,
              why,
              "Not rewritten: the result may become a named output column, and a cast would change its auto-generated " +
                s"name. spark.sql.legacy.createEmptyCollectionUsingStringType=true restores 2.4's type; or use $replacement " +
                "(from org.apache.spark.sql.types) and alias it .as(\"<the name 2.4 gave it>\") if the column name matters.",
              t
            )
          )
        }
    }.asPatch
  }
}
