package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class AccumulatorUpgrade extends SemanticRule("AccumulatorUpgrade") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val accumulatorFunMatch = SymbolMatcher.normalized("org.apache.spark.SparkContext.accumulator")
    val utils = new Utils()
    val ruleId = "AccumulatorUpgrade"

    // Not auto-rewritten: no typed accumulator preserves a non-zero initial
    // value, so the call is commented out (replaced with `null`) rather than
    // silently miscompiled -- this needs a human to pick an AccumulatorV2.
    def needsManualMigration(e: Tree): Patch =
      RuleFinding.report(
        RuleChange(
          ruleId,
          "sc.accumulator is removed in Spark 3.x, and no typed accumulator (longAccumulator/doubleAccumulator) accepts a non-zero/named initial value.",
          "Commented out the accumulator construction and replaced its use with null; needs manual migration to an AccumulatorV2 with the correct initial value.",
          e
        ),
        Seq(Patch.addLeft(e, "/*"), Patch.addRight(e, "*/ null")).asPatch
      )

    def matchOnTree(e: Tree): Patch = {
      e match {
        // non-named accumulator
        case ns @ Term.Apply(j @ accumulatorFunMatch(f), params) =>
          // Find the spark context for rewriting
          val sc = ns.children(0).children(0)
          params match {
            case List(param) =>
              param match {
                // TODO: Handle non zero values
                case utils.intMatcher(initialValue) =>
                  needsManualMigration(e)
                case q"0L" =>
                  RuleFinding.report(
                    RuleChange(
                      ruleId,
                      "sc.accumulator is removed in Spark 3.x; the typed accumulator API replaces it.",
                      s"Rewrote to ${sc}.longAccumulator",
                      ns
                    ),
                    Patch.replaceTree(ns, s"${sc}.longAccumulator")
                  )
                // A non-zero Long initial value: same situation as the Int
                // case above, so it needs the same manual-migration finding.
                // Returning Patch.empty here reported NOTHING at all -- no
                // rewrite and, since nothing calls RuleFinding.report, no
                // finding either -- for a call that does not exist in Spark
                // 3.x. A removed API silently missing from the analysis
                // report is worse than one flagged as needing manual work.
                case utils.longMatcher(initialValue) =>
                  needsManualMigration(e)
                case q"0.0" =>
                  RuleFinding.report(
                    RuleChange(
                      ruleId,
                      "sc.accumulator is removed in Spark 3.x; the typed accumulator API replaces it.",
                      s"Rewrote to ${sc}.doubleAccumulator",
                      ns
                    ),
                    Patch.replaceTree(ns, s"${sc}.doubleAccumulator")
                  )
                case _ =>
                  needsManualMigration(e)
              }
            case List(param, name) =>
              needsManualMigration(e)
            // Any other arity (an explicitly-passed AccumulableParam, a
            // shape we haven't seen) used to fall off the end of this match
            // and throw MatchError from inside the rule, which scalafix
            // surfaces as UnexpectedError and which fails the whole analysis
            // run for the repo. Flag it for a human instead.
            case _ =>
              needsManualMigration(e)
          }
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ => elem.children.map(matchOnTree).asPatch
          }
      }
    }
    matchOnTree(doc.tree)
  }
}
