package fix.support

import java.io.{FileWriter, PrintWriter}

import scalafix.v1._

import scala.meta._

/**
 * Static metadata for a rule, independent of any single finding it produces.
 * This is the single source of truth for tier/confidence/doc-link -- the
 * orchestrator (spark-migrate-cli) reads this via RuleRegistry rather than
 * hardcoding tier classifications on its own side.
 */
final case class RuleMeta(
    ruleId: String,
    tier: Int,
    description: String,
    docLink: String,
    defaultConfidence: String
)

object RuleRegistry {
  private val entries: Seq[RuleMeta] = Seq(
    RuleMeta(
      ruleId = "AccumulatorUpgrade",
      tier = 1,
      description = "SparkContext.accumulator is removed in Spark 3.x; migrate to the typed accumulator API (longAccumulator/doubleAccumulator) or a custom AccumulatorV2.",
      docLink = "https://spark.apache.org/docs/latest/core-migration-guide.html#upgrading-from-core-24-to-30",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "MigrateToSparkSessionBuilder",
      tier = 1,
      description = "SQLContext construction/getOrCreate is deprecated; migrate to SparkSession.builder().",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "GroupByKeyRenameColumnQQ",
      tier = 1,
      description = "Since Spark 3.0 Dataset.groupByKey(...).count() names the grouping column 'key' instead of 'value'; column references in the same file are rewritten to match.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "GroupByKeyRewrite",
      tier = 1,
      description = "Rewrites literal 'value' column references following a groupByKey(...).toDS().count() chain to 'key', matching the Spark 3.0 column-naming change.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "GroupByKeyWarn",
      tier = 3,
      description = "Flags Dataset.groupByKey(...).count() usage whose 'value'/'key' column naming behavior changed in Spark 3.0; the flagged pipeline needs manual review to confirm every reference was caught.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "UnionRewrite",
      tier = 1,
      description = "Dataset/DataFrame.unionAll is deprecated; use union instead (identical semantics).",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SqlInStringDetect",
      tier = 3,
      description = "Flags spark.sql(...)/sqlContext.sql(...) call sites so in-line SQL can be reviewed separately for Spark 3.x SQL-dialect changes; classifies the query as literal, string-interpolated, or dynamically built.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    )
  )

  val all: Map[String, RuleMeta] = entries.map(m => m.ruleId -> m).toMap

  /** Falls back to a Tier 3 "undocumented" entry rather than throwing, since a
   *  rule firing with no registry entry is a rule-authoring bug, not something
   *  that should crash the whole analysis run. */
  def apply(ruleId: String): RuleMeta =
    all.getOrElse(ruleId, RuleMeta(ruleId, tier = 3, description = "Undocumented rule.", docLink = "", defaultConfidence = "low"))
}

/**
 * One concrete finding a rule produced at a specific source location.
 * `explanation` is the "why does this matter" text, `change` is the "what
 * would/did change" text -- the two halves of the rough diagnostic format
 * agreed for this iteration (ruleId + explanation + what needs to change).
 */
final case class RuleChange(ruleId: String, why: String, change: String, position: Position) extends Diagnostic {
  override def message: String = s"$why\nChange: $change"
}

object RuleChange {
  def apply(ruleId: String, explanation: String, change: String, tree: Tree): RuleChange =
    RuleChange(ruleId, explanation, change, tree.pos)
}

/**
 * Append-only structured sink for findings, read by spark-migrate-cli's
 * Analysis/Code-Changes steps. Writing here is a no-op unless the CLI has
 * opted in via -Dspark.migrate.findingsSink=<path> (e.g. running under plain
 * `sbt scalafixAll`, scalafix-testkit, or any other ad-hoc use of this jar is
 * unaffected). Deliberately a flat, hand-built JSON line rather than a real
 * codec so this module stays dependency-light -- the schema is intentionally
 * rough for this iteration and expected to be consolidated later.
 */
object FindingsSink {
  private val sinkPath: Option[String] = sys.props.get("spark.migrate.findingsSink")

  private def jsonEscape(s: String): String =
    s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => ""
      case '\t' => "\\t"
      // Any other control character has to be escaped too, not passed
      // through: a raw 0x00-0x1F byte reaching this hand-built JSON line
      // makes the whole findings file unparseable on the consumer side, and
      // one bad character then loses every finding in the run.
      case c if c < ' ' => "\\u%04x".format(c.toInt)
      case c    => c.toString
    }

  def emit(rc: RuleChange)(implicit doc: SemanticDocument): Unit = sinkPath.foreach { path =>
    val pos = rc.position
    val line =
      "{" +
        s""""ruleId":"${jsonEscape(rc.ruleId)}",""" +
        s""""file":"${jsonEscape(doc.input.syntax)}",""" +
        s""""line":${pos.startLine + 1},""" +
        s""""column":${pos.startColumn + 1},""" +
        s""""explanation":"${jsonEscape(rc.why)}",""" +
        s""""change":"${jsonEscape(rc.change)}"""" +
        "}"
    FindingsSink.synchronized {
      val out = new PrintWriter(new FileWriter(path, true))
      try out.println(line)
      finally out.close()
    }
  }
}

/**
 * The single entry point rule bodies should call instead of building
 * `Patch.lint`/rewrite patches ad hoc. Every call both records a structured
 * finding (via FindingsSink) and returns a lint diagnostic -- so lint-mode
 * (Analysis, `--check`) and fix-mode (Code Changes) runs of the *same* rule
 * body produce the same findings; fix-mode additionally applies `rewrite`.
 */
object RuleFinding {
  def report(rc: RuleChange, rewrite: Patch = Patch.empty)(implicit doc: SemanticDocument): Patch = {
    FindingsSink.emit(rc)
    Seq(Patch.lint(rc), rewrite).asPatch
  }
}
