package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class AccumulatorUpgrade extends SemanticRule("AccumulatorUpgrade") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val accumulatorFunMatch = SymbolMatcher.normalized("org.apache.spark.SparkContext.accumulator")
    val accumulatorTypeMatch = SymbolMatcher.normalized("org.apache.spark.Accumulator")
    val accumulablePlusEqMatch = SymbolMatcher.normalized("org/apache/spark/Accumulable#`+=`().")
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

    def rewriteCall(t: Tree, replacement: String): Patch =
      RuleFinding.report(
        RuleChange(ruleId, "org.apache.spark.Accumulator is removed in Spark 3.x; the typed accumulator API replaces it.", s"Rewrote to $replacement", t),
        Patch.replaceTree(t, replacement)
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
            // A NAMED accumulator (`sc.accumulator(0L, "name")`) with a
            // trivial zero initial value is exactly as safe to rewrite as
            // the unnamed 1-arg case above -- `longAccumulator`/
            // `doubleAccumulator` both take the name directly. This case
            // used to unconditionally call needsManualMigration even for
            // the 0L/0.0 shape, which left a real 2.4.8 codebase (this
            // fork's own end-to-end fixture) needing manual work for a
            // call that has an exact, safe typed-accumulator equivalent.
            case List(param, name) =>
              param match {
                case q"0L" => rewriteCall(ns, s"${sc}.longAccumulator($name)")
                case q"0.0" => rewriteCall(ns, s"${sc}.doubleAccumulator($name)")
                case _ => needsManualMigration(e)
              }
            // Any other arity (an explicitly-passed AccumulableParam, a
            // shape we haven't seen) used to fall off the end of this match
            // and throw MatchError from inside the rule, which scalafix
            // surfaces as UnexpectedError and which fails the whole analysis
            // run for the repo. Flag it for a human instead.
            case _ =>
              needsManualMigration(e)
          }

        // `accumulator += value` (Accumulable's `+=`, an infix operator call
        // resolved against 2.4.8's own semanticdb -- confirmed via a debug
        // probe against this fork's own end-to-end fixture, which has
        // exactly this pattern one call site away from the accumulator's
        // own construction) has no `+=` equivalent on any of the 3.x typed
        // accumulators (AccumulatorV2's API is `add`, not an operator) --
        // `.add(...)` is the one call every typed accumulator (Long/Double/
        // Collection/custom AccumulatorV2) defines, so this rewrite is safe
        // regardless of which typed accumulator the construction above
        // became. Not gated on "did the construction get safely rewritten":
        // if it didn't (the value is now `Any`), this call site was already
        // going to fail to compile on its receiver's type, independent of
        // whether `+=`/`.add(...)` is the operator used.
        case t @ Term.ApplyInfix(recv, accumulablePlusEqMatch(_), _, List(arg)) =>
          rewriteCall(t, s"$recv.add($arg)")

        // `org.apache.spark.Accumulator[T]` itself is removed, independent
        // of how (or whether) any given call site above got rewritten --
        // `import org.apache.spark.Accumulator` and any `Accumulator[T]`
        // type annotation are separate hard compile breaks that survive
        // even a fully-successful call-site rewrite. Long/Double get the
        // same safety judgment as the call-site zero-value case (an exact,
        // unambiguous typed-accumulator equivalent); fully-qualified rather
        // than adding an import, since Patch.addGlobalImport is documented
        // elsewhere in this jar (MigrateHiveContext) as broken in practice.
        case t @ Type.Apply(accumulatorTypeMatch(_), List(Type.Name("Long"))) =>
          rewriteCall(t, "org.apache.spark.util.LongAccumulator")
        case t @ Type.Apply(accumulatorTypeMatch(_), List(Type.Name("Double"))) =>
          rewriteCall(t, "org.apache.spark.util.DoubleAccumulator")
        // Any other type argument (Int, a case class, etc.) has no single
        // exact typed-accumulator equivalent -- AccumulatorV2/
        // LongAccumulator/DoubleAccumulator/CollectionAccumulator all differ
        // in shape. Replaced with `Any` rather than left as `Accumulator[T]`
        // (which no longer exists at all in Spark 3.x, so leaving it verbatim
        // wouldn't compile) or a bare `Patch.empty` (same problem) -- the
        // same "neutralize to something that at least compiles" approach
        // `needsManualMigration` already uses for the call site itself.
        case t @ Type.Apply(accumulatorTypeMatch(_), _) =>
          RuleFinding.report(
            RuleChange(
              ruleId,
              "org.apache.spark.Accumulator is removed in Spark 3.x and this usage has no single typed-accumulator equivalent.",
              "Replaced with Any as a placeholder; migrate to AccumulatorV2/LongAccumulator/DoubleAccumulator/CollectionAccumulator by hand.",
              t
            ),
            Patch.replaceTree(t, "Any")
          )
        // A bare `Accumulator` reference with no type argument at all --
        // same treatment as above.
        case t @ accumulatorTypeMatch(_) =>
          RuleFinding.report(
            RuleChange(
              ruleId,
              "org.apache.spark.Accumulator is removed in Spark 3.x and this usage has no single typed-accumulator equivalent.",
              "Replaced with Any as a placeholder; migrate to AccumulatorV2/LongAccumulator/DoubleAccumulator/CollectionAccumulator by hand.",
              t
            ),
            Patch.replaceTree(t, "Any")
          )

        // The import itself is a hard break independent of any call-site or
        // type rewrite above: `import org.apache.spark.Accumulator` fails
        // to compile in Spark 3.x regardless of whether Accumulator is
        // still referenced anywhere else in the file, since the class no
        // longer exists in org.apache.spark at all. Safe to remove
        // unconditionally: every type-level usage above is rewritten to
        // something else (LongAccumulator/DoubleAccumulator/Any), so nothing
        // in the file still needs this import after this rule runs.
        case imp @ Import(List(Importer(_, importees))) if importees.exists {
              case Importee.Name(Name("Accumulator")) => true
              case Importee.Rename(Name("Accumulator"), _) => true
              case _ => false
            } =>
          importees.collect {
            case i @ Importee.Name(Name("Accumulator")) =>
              RuleFinding.report(
                RuleChange(ruleId, "org.apache.spark.Accumulator is removed in Spark 3.x; this import no longer compiles.", "Removed the Accumulator import.", imp),
                Patch.removeImportee(i)
              )
            case i @ Importee.Rename(Name("Accumulator"), _) =>
              RuleFinding.report(
                RuleChange(ruleId, "org.apache.spark.Accumulator is removed in Spark 3.x; this import no longer compiles.", "Removed the Accumulator import.", imp),
                Patch.removeImportee(i)
              )
            case _ => Patch.empty
          }.asPatch

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
