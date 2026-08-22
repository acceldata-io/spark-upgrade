package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.util.matching.Regex

/**
 * Tier 2 / Class A detector (design review Part 2 Family B / Part 4): the
 * SQL `SET key=value` command now fails, instead of silently no-op'ing, when
 * `key` is a core (non-`spark.sql.*`) Spark config -- Spark 3.0's default
 * `spark.sql.legacy.setCommandRejectsSparkCoreConfs=true`. A SQL-text
 * concern (the `SET` command lives inside an embedded SQL string), so this
 * is a regex scan over the literal argument of `spark.sql(...)`/
 * `sqlContext.sql(...)` calls -- the same call sites `SqlInStringDetect`
 * finds -- rather than a Scala AST match, mirroring
 * `MixedIntervalLiteralDetect`'s approach to the same class of problem.
 *
 * Feeds `spark.sql.legacy.setCommandRejectsSparkCoreConfs`, but the
 * registry's own remediation note prefers setting the key at submit time
 * (spark-defaults.conf / --conf) over injecting the legacy flag.
 */
class SetCommandSparkConfDetect extends SemanticRule("SetCommandSparkConfDetect") {
  override val description =
    "Flags a SQL `SET key=value` command targeting a non-spark.sql.* config key, which Spark 3.0 rejects by default instead of silently no-op'ing."

  private val sqlMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.SparkSession.sql",
    "org.apache.spark.sql.SQLContext.sql"
  )

  // "SET some.key = value" / "SET some.key=value" -- the key is the first
  // token after SET up to (not including) the `=`; `set key` (no `=`, a
  // query rather than an assignment) is deliberately not matched, since
  // that form only ever reads a value and can't hit this behavior change.
  private val setCommand: Regex = """(?im)^\s*SET\s+([^\s=]+)\s*=""".r

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case Term.Apply(sqlMatcher(_), List(lit @ Lit.String(sql))) =>
        setCommand
          .findAllMatchIn(sql)
          .collect {
            case m if !m.group(1).startsWith("spark.sql.") =>
              RuleFinding.report(
                RuleChange(
                  "SetCommandSparkConfDetect",
                  s"SET ${m.group(1)}=... targets a non-spark.sql.* config key in embedded SQL; Spark 3.0's default policy rejects SET on core Spark config keys instead of silently no-op'ing.",
                  "No auto-rewrite; set this key at submit time (spark-defaults.conf / --conf) instead of via SQL SET, or inject spark.sql.legacy.setCommandRejectsSparkCoreConfs=false.",
                  lit
                )
              )
          }
          .toSeq
          .asPatch
    }.asPatch
  }
}
