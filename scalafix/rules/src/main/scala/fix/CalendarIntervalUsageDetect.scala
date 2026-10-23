package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Spark 3.0 changed `org.apache.spark.unsafe.types.CalendarInterval` (javap on the 2.4.8 and 3.5.5
 * jars): a `days` field was added, and the `(months, microseconds)` constructor, the
 * `MICROS_PER_*` constants, the `from*String` parsers, `milliseconds()`, `add`, `subtract` and
 * `negate` were removed. (3.2's `DayTimeIntervalType` for date subtraction does not reach a 2.4
 * app: 2.4.8 rejects `date - date`, probed.)
 *
 * The obvious detector -- "find a subtraction between two date/timestamp
 * columns" -- needs both operand types resolved, which is the class of rule this
 * project has deliberately declined to build (no reliable type inference at the
 * call site). But the code that actually BREAKS is narrower and exactly
 * detectable: code that names `org.apache.spark.unsafe.types.CalendarInterval`,
 * because that is what stops being the result type. Verified present in the real
 * `spark-unsafe_2.11-2.4.8.jar`.
 *
 * So this flags the type reference rather than the arithmetic -- a smaller, exact
 * signal instead of a broad, uncertain one. It will miss a subtraction whose
 * result is never named (nothing to detect there anyway, and the schema change
 * would surface in Data Validation) and it will not false-positive.
 *
 * One shape has an exact rewrite (Tier 1, 3.x-only output, so
 * `targetOnly`): the constructor. 3.0 added a `days` field and removed the
 * `(int months, long microseconds)` constructor, so `new CalendarInterval(m, us)`
 * becomes `new CalendarInterval(m, 0, us)`. Probed on 2.4.8 and 3.5.5: the same
 * `months`/`microseconds` fields, the same `timestamp + interval` and
 * `date + interval` results and the same equality; only `toString` differs,
 * which is the 3.x class itself, not the rewrite. Every other reference is
 * reported as CalendarIntervalUsageDetect.Manual (Tier 3): code that reads the
 * fields of an interval Spark produced sees 3.x's split into days and
 * microseconds.
 */
class CalendarIntervalUsageDetect extends SemanticRule("CalendarIntervalUsageDetect") {
  override val description =
    "Rewrites the removed CalendarInterval(months, microseconds) constructor and flags other CalendarInterval references, whose field layout changed in Spark 3.0."

  private val ruleId = "CalendarIntervalUsageDetect"
  private val manualId = s"$ruleId.Manual"

  private val matcher = SymbolMatcher.normalized("org.apache.spark.unsafe.types.CalendarInterval")

  // The Type.Name of `new CalendarInterval(m, us)` with two positional arguments.
  // The type may be written qualified: `new org.apache.spark.unsafe.types.CalendarInterval(m, us)`.
  private def twoArgConstructor(t: Type.Name): Option[Term] = {
    val tpeTree: Type = t.parent match {
      case Some(sel @ Type.Select(_, n)) if n eq t => sel
      case _ => t
    }
    tpeTree.parent match {
      case Some(init @ Init(tpe, _, List(List(months, micros)))) if (tpe eq tpeTree) && !months.isInstanceOf[Term.Assign] &&
          !micros.isInstanceOf[Term.Assign] && init.parent.exists(_.isInstanceOf[Term.New]) =>
        Some(months)
      case _ => None
    }
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Type.Name("CalendarInterval") if matcher.matches(t) =>
        twoArgConstructor(t) match {
          case Some(months) =>
            RuleFinding.report(
              RuleChange(
                ruleId,
                "The CalendarInterval(months, microseconds) constructor was removed in Spark 3.0, which added a separate days field.",
                "Rewrote to new CalendarInterval(months, 0, microseconds): the same interval, with no days part.",
                t
              ),
              Patch.addRight(months, ", 0")
            )
          case None => report(t)
        }
      case t @ Term.Name("CalendarInterval") if matcher.matches(t) => report(t)
    }.asPatch
  }

  private def report(t: Tree)(implicit doc: SemanticDocument): Patch =
    RuleFinding.report(
      RuleChange(
        manualId,
        "CalendarInterval is referenced here. Spark 3.0 changed the class: it added a days field (`interval 1 day 2 hours` " +
          "is days=1, microseconds=7200000000, where 2.4 had microseconds=93600000000) and removed the (months, microseconds) " +
          "constructor, the MICROS_PER_* constants, the from*String parsers, milliseconds(), add, subtract and negate. Code " +
          "that calls those no longer compiles; code that reads microseconds silently gets different numbers.",
        "No exact rewrite beyond the constructor. Read days alongside microseconds; use java.util.concurrent.TimeUnit for " +
          "the constants; parse an interval in SQL (expr(\"interval ...\")) instead of fromString.",
        t
      )
    )
}
