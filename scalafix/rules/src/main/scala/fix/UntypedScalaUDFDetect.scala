package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A detector (design review Part 4): the 2-arg
 * `functions.udf(f: AnyRef, dataType: DataType)` form is deprecated in
 * Spark 3.x and, per the migration guide, also changes null semantics (a
 * null input used to return null; now it returns the return type's zero
 * value). Every typed `udf[...]` overload takes exactly ONE value argument
 * (the function literal); only this untyped form takes two, which is what
 * makes a plain arity check on the matched call sufficient here -- no
 * deeper type inspection needed to tell the two apart.
 *
 * Feeds `spark.sql.legacy.allowUntypedScalaUDF` (LegacyConfigRegistry), but
 * the registry's own remediation note is explicit that rewriting to a typed
 * UDF is preferred over injecting the config, since the config alone does
 * not restore the old null-handling behavior.
 */
class UntypedScalaUDFDetect extends SemanticRule("UntypedScalaUDFDetect") {
  override val description =
    "Flags the deprecated 2-arg functions.udf(AnyRef, DataType) form, which also changed null-handling semantics in Spark 3.0."

  private val udfMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.udf")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(udfMatcher(_), List(_, _)) =>
        RuleFinding.report(
          RuleChange(
            "UntypedScalaUDFDetect",
            "functions.udf(AnyRef, DataType) -- the untyped 2-arg form -- is deprecated in Spark 3.0 and also changed null-handling: " +
              "a null input used to return null, now it returns the return type's zero value.",
            "No auto-rewrite; prefer rewriting to a typed UDF over injecting spark.sql.legacy.allowUntypedScalaUDF, " +
              "since the config alone doesn't restore the old null semantics.",
            t
          )
        )
    }.asPatch
  }
}
