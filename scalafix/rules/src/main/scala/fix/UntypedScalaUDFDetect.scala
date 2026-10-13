package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector: the untyped `functions.udf(f: AnyRef, dataType: DataType)`
 * form throws at udf creation on Spark 3.x unless
 * `spark.sql.legacy.allowUntypedScalaUDF=true`, and that config does not
 * restore 2.4: probed, `udf((x: Int) => x + 1, IntegerType)` on [1, null]
 * gives [2, null] on 2.4.8 but [2, 1] on 3.5.5 with the config (a null
 * becomes 0 for a primitive parameter). The config is job-global, so it is
 * never injected; the fix is a typed UDF. Every typed `udf[...]` overload
 * takes exactly ONE value argument and only this form takes two, so an arity
 * check on the matched call tells them apart.
 */
class UntypedScalaUDFDetect extends SemanticRule("UntypedScalaUDFDetect") {
  override val description =
    "Flags the untyped functions.udf(AnyRef, DataType) form, which Spark 3.x rejects; rewrite it as a typed UDF."

  private val udfMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.udf")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(udfMatcher(_), List(_, _)) =>
        RuleFinding.report(
          RuleChange(
            "UntypedScalaUDFDetect",
            "functions.udf(AnyRef, DataType) -- the untyped form -- throws at udf creation on Spark 3.x. " +
              "spark.sql.legacy.allowUntypedScalaUDF lets it run, but then a null input to a primitive parameter becomes 0 " +
              "(udf((x: Int) => x + 1, IntegerType) gives 1 for null, where 2.4 gave null).",
            "Rewrite as a typed UDF, e.g. udf((x: Int) => x + 1) or udf[Int, Int](...), which returns null for a null " +
              "primitive input as 2.4 did; use a boxed type (java.lang.Integer) or Option if the function must see the null. " +
              "Do not set spark.sql.legacy.allowUntypedScalaUDF: it silently turns null into 0 for primitive parameters.",
            t
          )
        )
    }.asPatch
  }
}
