package fix

import java.time.ZoneId

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.util.Try

/**
 * Family D (SQL migration guide, 2.4 -> 3.0): an invalid timezone ID used to
 * fall back silently to GMT; from 3.0 it throws. A literal timezone string is
 * exactly decidable, so this is a rare Family D item that needs no type
 * inference at all.
 *
 * The validator is the one Spark itself uses, and getting that right was the
 * whole difficulty. Spark 3.x resolves a timezone through
 * `DateTimeUtils.getZoneId`, which honours `java.time.ZoneId.SHORT_IDS` --
 * probed against the real 3.5.5 jar, `EST`, `PST`, `CST`, `MST` and `IST` all
 * RESOLVE FINE (to -05:00, America/Los_Angeles, America/Chicago, -07:00 and
 * Asia/Kolkata respectively). A rule that flagged three-letter abbreviations as
 * "not a valid zone ID" -- the obvious first guess -- would therefore have been
 * wrong on the most common ones. Only genuinely unknown IDs are rejected
 * (`PDT` and `Not/AZone` both throw `ZoneRulesException`).
 *
 * So: accept exactly what `ZoneId.of(id, ZoneId.SHORT_IDS)` accepts. Same
 * function, same answer, no false positives by construction. Pure JDK, so this
 * rule needs nothing from Spark on its own classpath.
 */
class InvalidTimeZoneIdDetect extends SemanticRule("InvalidTimeZoneIdDetect") {
  override val description =
    "Flags a literal timezone ID that Spark 3.x rejects; 2.4 silently fell back to GMT instead of throwing."

  private val tzFunMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.functions.from_utc_timestamp",
    "org.apache.spark.sql.functions.to_utc_timestamp"
  )

  private val sessionTimeZoneKey = "spark.sql.session.timeZone"

  private def isResolvable(id: String): Boolean =
    Try(ZoneId.of(id, ZoneId.SHORT_IDS)).isSuccess

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      // from_utc_timestamp(col, "<tz>") / to_utc_timestamp(col, "<tz>")
      case Term.Apply(tzFunMatcher(_), args) =>
        args.collect {
          case lit @ Lit.String(tz) if !isResolvable(tz) => report(lit, tz, "passed to a *_utc_timestamp function")
        }.asPatch

      // .config("spark.sql.session.timeZone", "<tz>") / .set(...) -- purely
      // syntactic on purpose: the receiver may be a SparkConf, a
      // SparkSession.Builder or a RuntimeConfig, and the key literal is already
      // an unambiguous signal without resolving which.
      case Term.Apply(Term.Select(_, Term.Name("config" | "set")), List(Lit.String(k), lit @ Lit.String(tz)))
          if k == sessionTimeZoneKey && !isResolvable(tz) =>
        report(lit, tz, s"""set as $sessionTimeZoneKey""")
    }.asPatch
  }

  private def report(lit: Lit.String, tz: String, where: String)(implicit doc: SemanticDocument): Patch =
    RuleFinding.report(
      RuleChange(
        "InvalidTimeZoneIdDetect",
        s"""Timezone ID "$tz" ($where) is not resolvable by java.time (checked with ZoneId.of(id, ZoneId.SHORT_IDS), """ +
          "the same resolution Spark performs). Spark 2.4 silently fell back to GMT for an unrecognised ID; " +
          "Spark 3.0 and later throw instead -- so this is a latent hard failure, and any data produced under 2.4 " +
          "was computed in GMT rather than the intended zone.",
        "No auto-rewrite -- remapping an abbreviation to a region ID would silently change daylight-saving " +
          "behaviour. Replace it with an IANA region ID (e.g. America/New_York) after confirming what was intended.",
        lit
      )
    )
}
