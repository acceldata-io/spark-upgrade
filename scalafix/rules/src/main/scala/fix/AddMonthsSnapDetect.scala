package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 3 detector (design review Part 2 Family D): `add_months` no longer
 * snaps the result to the last day of the month when the input date is
 * itself the last day of its month (e.g. `add_months('2019-02-28', 1)` no
 * longer reliably lands on the last day of March the way 2.4 did). This is a
 * silent numeric/data drift with no legacy config to restore the old
 * behavior, so every call site is flagged unconditionally -- whether it
 * actually matters depends on whether the input dates are ever month-end,
 * which is data, not code.
 */
class AddMonthsSnapDetect extends SemanticRule("AddMonthsSnapDetect") {
  override val description =
    "Flags add_months(...) call sites -- Spark 3.0 no longer reliably snaps the result to the last day of the month for month-end inputs."

  private val addMonthsMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.add_months")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(addMonthsMatcher(_), _) =>
        RuleFinding.report(
          RuleChange(
            "AddMonthsSnapDetect",
            "add_months(...) call found -- Spark 3.0 no longer reliably snaps the result to the last day of the month when the input date is a month-end date (e.g. 2019-02-28 + 1 month). Silent, data-dependent numeric drift.",
            "No auto-rewrite and no legacy config restores this -- review downstream logic that assumes end-of-month snapping if the input dates can be month-end.",
            t
          )
        )
    }.asPatch
  }
}
