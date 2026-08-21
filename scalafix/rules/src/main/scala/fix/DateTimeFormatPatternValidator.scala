package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A detector (design review Part 2 Family D / Part 4):
 * flags datetime pattern-string literals passed to
 * `date_format`/`to_date`/`to_timestamp`/`unix_timestamp`/`from_unixtime`
 * that use letters whose behavior actually changed under Spark 3.0's switch
 * from `SimpleDateFormat` to `DateTimeFormatter`. Deliberately narrow and
 * high-precision rather than a full compatibility validator (verifying the
 * "whole input must parse" change needs the runtime data, not just the
 * pattern string) -- it only flags the two well-documented, purely
 * syntactic traps:
 *
 *   - `hh` (1-12 clock hour) used without an `a` (am/pm marker) anywhere in
 *     the same pattern -- ambiguous under 2.4's lenient parser, and exactly
 *     the shape of the migration guide's own example
 *     (`dd/MM/yyyy hh:mm` failing to parse `31/01/2015 00:00`).
 *   - `F` (week-of-month in 2.4, aligned-day-of-week-in-month in 3.0+) --
 *     any use of this letter silently changes meaning.
 *
 * Feeds `spark.sql.legacy.timeParserPolicy`, but the registry's own
 * remediation note prefers correcting the pattern (e.g. `hh`->`HH`) over
 * injecting `LEGACY`, since the validator can often statically tell which
 * patterns actually changed behavior.
 */
class DateTimeFormatPatternValidator extends SemanticRule("DateTimeFormatPatternValidator") {
  override val description =
    "Flags datetime format pattern strings using letters whose meaning changed under Spark 3.0's DateTimeFormatter."

  private val dateTimeFunMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.date_format",
    "org.apache.spark.sql.functions.to_date",
    "org.apache.spark.sql.functions.to_timestamp",
    "org.apache.spark.sql.functions.unix_timestamp",
    "org.apache.spark.sql.functions.from_unixtime"
  )

  private def flagReason(pattern: String): Option[String] =
    if (pattern.contains("hh") && !pattern.contains("a"))
      Some("uses 12-hour clock letter 'hh' with no 'a' (am/pm) marker in the same pattern -- ambiguous, and Spark 3.x's stricter parser can reject input this pattern used to silently mis-parse")
    else if (pattern.contains("F"))
      Some("uses pattern letter 'F', which meant week-of-month before Spark 3.0 and means aligned-day-of-week-in-month from 3.0 onward -- same pattern, different result")
    else None

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case Term.Apply(dateTimeFunMatcher(_), args) =>
        args.collectFirst { case lit @ Lit.String(pattern) => (lit, pattern) } match {
          case Some((lit, pattern)) =>
            flagReason(pattern) match {
              case Some(reason) =>
                RuleFinding.report(
                  RuleChange(
                    "DateTimeFormatPatternValidator",
                    s"""Pattern "$pattern" $reason.""",
                    "No auto-rewrite; correct the pattern (preferred) or inject spark.sql.legacy.timeParserPolicy=LEGACY.",
                    lit
                  )
                )
              case None => Patch.empty
            }
          case None => Patch.empty
        }
    }.asPatch
  }
}
