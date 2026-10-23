package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * The untyped `functions.udf(f: AnyRef, dataType: DataType)` form throws at
 * udf creation on Spark 3.x unless `spark.sql.legacy.allowUntypedScalaUDF=true`,
 * and that config does not restore 2.4: probed, `udf((x: Int) => x + 1,
 * IntegerType)` on [1, null] gives [2, null] on 2.4.8 but [2, 1] on 3.5.5 with
 * the config (a null becomes 0 for a primitive parameter). The config is
 * job-global, so it is never injected; the fix is a typed UDF. Every typed
 * `udf[...]` overload takes exactly ONE value argument and only this form
 * takes two, so an arity check on the matched call tells them apart.
 *
 * Tier 1 where the typed form is exact: `f` is a lambda whose
 * every parameter is annotated with a primitive, boxed primitive, String,
 * java.sql.Date or java.sql.Timestamp, and the DataType is one of the matching
 * atomic types. `udf(f, IntegerType)` becomes `udf[java.lang.Integer, A1, ...](f)`.
 * The result type is the boxed one so that a body returning null still
 * compiles, and its schema is `nullable = true` like 2.4's untyped UDF. Probed
 * on 2.4.8 (untyped) and 3.5.5 (typed) for Int, Long, Double, Boolean, String,
 * two-parameter and Date UDFs, on null and non-null input: the same column name
 * (`UDF(x)`), type, nullability and values. A body whose type does not convert
 * to the result type fails to compile after the rewrite; on 2.4.8 such a UDF
 * threw ClassCastException when it ran (probed: an Int body for LongType).
 * Every other call is reported as UntypedScalaUDFDetect.Manual (Tier 3).
 */
class UntypedScalaUDFDetect extends SemanticRule("UntypedScalaUDFDetect") {
  override val description =
    "Rewrites the untyped functions.udf(AnyRef, DataType) form, which Spark 3.x rejects, to the typed UDF where that is exact."

  private val ruleId = "UntypedScalaUDFDetect"
  private val manualId = s"$ruleId.Manual"

  private val udfMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.udf")

  // DataType object -> the typed UDF's result type.
  private val resultTypes: Map[String, String] = Map(
    "IntegerType" -> "java.lang.Integer",
    "LongType" -> "java.lang.Long",
    "DoubleType" -> "java.lang.Double",
    "FloatType" -> "java.lang.Float",
    "ShortType" -> "java.lang.Short",
    "ByteType" -> "java.lang.Byte",
    "BooleanType" -> "java.lang.Boolean",
    "StringType" -> "String",
    "DateType" -> "java.sql.Date",
    "TimestampType" -> "java.sql.Timestamp"
  )

  // The case object in org.apache.spark.sql.types, or the DataTypes.X static field.
  private def resultType(dt: Term)(implicit doc: SemanticDocument): Option[String] = {
    val sym = dt.symbol.value
    resultTypes.collectFirst {
      case (name, tpe) if sym == s"org/apache/spark/sql/types/$name." || sym == s"org/apache/spark/sql/types/DataTypes#$name." => tpe
    }
  }

  private val paramTypes: Set[String] = Set(
    "scala/Int#", "scala/Long#", "scala/Double#", "scala/Float#", "scala/Short#", "scala/Byte#", "scala/Boolean#",
    "java/lang/Integer#", "java/lang/Long#", "java/lang/Double#", "java/lang/Float#", "java/lang/Short#",
    "java/lang/Byte#", "java/lang/Boolean#",
    "java/lang/String#", "scala/Predef.String#", "java/sql/Date#", "java/sql/Timestamp#"
  )

  private def isSimpleParam(p: Term.Param)(implicit doc: SemanticDocument): Boolean =
    p.decltpe.isDefined && p.name.symbol.info.exists(_.signature match {
      case ValueSignature(TypeRef(_, sym, Nil)) => paramTypes(sym.value)
      case _ => false
    })

  private val explanation =
    "functions.udf(AnyRef, DataType) -- the untyped form -- throws at udf creation on Spark 3.x. " +
      "spark.sql.legacy.allowUntypedScalaUDF lets it run, but then a null input to a primitive parameter becomes 0 " +
      "(udf((x: Int) => x + 1, IntegerType) gives 1 for null, where 2.4 gave null)."

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(fun @ udfMatcher(_), List(f, dt)) =>
        (f, resultType(dt)) match {
          case (Term.Function(params, _), Some(r)) if params.size <= 10 && params.forall(isSimpleParam) =>
            val typeArgs = (r :: params.flatMap(_.decltpe).map(_.syntax)).mkString(", ")
            val replacement = s"$fun[$typeArgs]($f)"
            RuleFinding.report(
              RuleChange(ruleId, explanation,
                s"Rewrote to the typed UDF $fun[$typeArgs](...), which gives 2.4's result: null for a null primitive input, " +
                  "the same column name and type.", t),
              Patch.replaceTree(t, replacement)
            )
          case _ =>
            RuleFinding.report(
              RuleChange(
                manualId,
                explanation,
                "Not rewritten: the tool rewrites only a lambda with every parameter type written out (a primitive, String, " +
                  "java.sql.Date or java.sql.Timestamp) and a simple DataType such as IntegerType. Rewrite it as a typed UDF, " +
                  "e.g. udf[java.lang.Integer, Int]((x: Int) => x + 1), which returns null for a null primitive input as 2.4 did; " +
                  "use a boxed type (java.lang.Integer) or Option if the function must see the null. " +
                  "Do not set spark.sql.legacy.allowUntypedScalaUDF: it silently turns null into 0 for primitive parameters.",
                t
              )
            )
        }
    }.asPatch
  }
}
