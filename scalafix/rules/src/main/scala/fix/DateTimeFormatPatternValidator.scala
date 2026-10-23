package fix

import fix.support.{DateTimePatterns, RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A detector (Family D):
 * flags datetime pattern-string literals passed to
 * `date_format`/`to_date`/`to_timestamp`/`unix_timestamp`/`from_unixtime`
 * that Spark 3.0's switch from `SimpleDateFormat` to `DateTimeFormatter`
 * treats differently. It applies `DateTimePatterns.check`, the PySpark rule's probed
 * table: week-based letters (`Y w W u`), parsing with `E`/`F`, five-letter
 * text forms, DateTimeFormatter's letter-count limits, the reserved `# { }`
 * and `[ ]`, `F` when formatting and `h` without `a` when parsing. Verifying
 * the "whole input must parse" change needs the data, not the pattern, so
 * that is left to the baseline config.
 *
 * Feeds `spark.sql.legacy.timeParserPolicy`, but the registry's own
 * remediation note prefers correcting the pattern (e.g. `hh`->`HH`) over
 * injecting `LEGACY`.
 */
class DateTimeFormatPatternValidator extends SemanticRule("DateTimeFormatPatternValidator") {
  override val description =
    "Flags datetime format pattern strings that Spark 3.0's DateTimeFormatter rejects or reads differently from 2.4's SimpleDateFormat."

  private val parsingMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.to_date",
    "org.apache.spark.sql.functions.to_timestamp",
    "org.apache.spark.sql.functions.unix_timestamp"
  )
  private val formattingMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.date_format",
    "org.apache.spark.sql.functions.from_unixtime"
  )

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case Term.Apply(fun, args) if parsingMatcher.matches(fun) || formattingMatcher.matches(fun) =>
        args.collectFirst { case lit @ Lit.String(pattern) => (lit, pattern) } match {
          case Some((lit, pattern)) =>
            DateTimePatterns.check(pattern, parsing = parsingMatcher.matches(fun)) match {
              case Some(reason) =>
                RuleFinding.report(
                  RuleChange(
                    "DateTimeFormatPatternValidator",
                    s"""Pattern "$pattern" $reason. 2.4 used SimpleDateFormat; Spark 3.x uses DateTimeFormatter.""",
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
