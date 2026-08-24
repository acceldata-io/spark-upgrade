package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Family D (SQL migration guide, 3.1 -> 3.2): date and timestamp subtraction
 * returns `DayTimeIntervalType` from 3.2 onward, where 2.4 returned
 * `CalendarIntervalType`.
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
 */
class CalendarIntervalUsageDetect extends SemanticRule("CalendarIntervalUsageDetect") {
  override val description =
    "Flags references to CalendarInterval -- from Spark 3.2 date/timestamp subtraction yields DayTimeIntervalType instead, so code typed against CalendarInterval breaks."

  private val matcher = SymbolMatcher.normalized("org.apache.spark.unsafe.types.CalendarInterval")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Type.Name("CalendarInterval") if matcher.matches(t) => report(t)
      case t @ Term.Name("CalendarInterval") if matcher.matches(t) => report(t)
    }.asPatch
  }

  private def report(t: Tree)(implicit doc: SemanticDocument): Patch =
    RuleFinding.report(
      RuleChange(
        "CalendarIntervalUsageDetect",
        "CalendarInterval is referenced here. From Spark 3.2, subtracting two dates or two timestamps produces " +
          "DayTimeIntervalType, not CalendarIntervalType -- so code that stores, pattern-matches or casts the " +
          "result as CalendarInterval no longer type-checks, and `date + <day-time interval>` now yields a " +
          "timestamp rather than a date.",
        "No auto-rewrite; migrate to the ANSI interval types (DayTimeIntervalType / YearMonthIntervalType), or " +
          "inject spark.sql.legacy.interval.enabled to keep the 2.4 interval behaviour.",
        t
      )
    )
}
