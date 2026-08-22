package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A detector (design review Part 2 Family G / Part 4):
 * since Spark 3.0, `spark.sql.analyzer.failAmbiguousSelfJoin` fails an
 * ambiguous self-join at analysis time instead of silently resolving column
 * references (sometimes to the wrong side). Narrowed to the unambiguous,
 * zero-false-positive case: `df.join(df, ...)` where the join receiver and
 * the first argument (the other side of every `Dataset.join` overload)
 * resolve to the exact same symbol -- checked via semantic symbol equality
 * rather than a hardcoded method symbol for "the other side," since that's
 * the only part of this pattern that isn't already covered by simply naming
 * `Dataset.join` in the matcher.
 *
 * Feeds `spark.sql.analyzer.failAmbiguousSelfJoin`, but the registry's own
 * remediation note prefers aliasing one side (e.g. `df.as("a").join(df.as("b"), ...)`)
 * over disabling the check.
 */
class SelfJoinAmbiguousColumnDetect extends SemanticRule("SelfJoinAmbiguousColumnDetect") {
  override val description =
    "Flags df.join(df, ...) self-joins using the exact same DataFrame reference on both sides, which Spark 3.0 can fail at analysis time as ambiguous."

  private val joinMatcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.join")

  private def sameReference(a: Tree, b: Tree)(implicit doc: SemanticDocument): Boolean = {
    val sa = a.symbol
    sa != Symbol.None && sa == b.symbol
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(recv, joinMatcher(_)), args) if args.nonEmpty && sameReference(recv, args.head) =>
        RuleFinding.report(
          RuleChange(
            "SelfJoinAmbiguousColumnDetect",
            "df.join(df, ...) joins a DataFrame to itself using the exact same reference on both sides; Spark 3.0's analyzer can fail this as an ambiguous self-join instead of resolving column references as 2.4 did.",
            "No auto-rewrite; alias one side (e.g. df.as(\"a\").join(df.as(\"b\"), ...)) instead of injecting spark.sql.analyzer.failAmbiguousSelfJoin=false.",
            t
          )
        )
    }.asPatch
  }
}
