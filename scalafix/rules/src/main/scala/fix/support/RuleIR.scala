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
      // Promoted back to Tier 1 (2026-08-22): the rule now handles the
      // three ways this hard break actually shows up in real code --
      // the call site (0L/0.0 constructor forms, named or not), the
      // Accumulator[Long]/[Double] type annotation, and the now-invalid
      // `import org.apache.spark.Accumulator` -- and every case it can't
      // safely resolve (non-zero initial value, Int/other type arg)
      // neutralizes to compilable placeholder code (comment-out+null /
      // `Any`) with a finding attached, rather than leaving a broken
      // reference. That's the same bar UnionRewrite's plain rename meets:
      // Phase A can apply it unattended without ever leaving the working
      // copy in a state that fails to compile worse than before.
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
    ),
    // Everything below was previously in the jar but had no RuleRegistry
    // entry, so it never ran in Analysis (RuleDiscovery.allRuleIds() is what
    // enables it there now) and never produced a structured finding even
    // when it ran in Code Changes -- see RuleFinding.report at each rule's
    // fire site. Defaulted to Tier 3 (manual review): Phase A auto-applies
    // any Tier 1 rule with no other safety gate, and none of these have been
    // exercised through this pipeline before. Promote individually once
    // proven safe on real repos.
    RuleMeta(
      ruleId = "RDDToDatasetMigrationCheck",
      tier = 3,
      description = "Checks whether a file's RDD usage is simple enough (every operation has a direct Dataset/DataFrame equivalent) to migrate to the typed Dataset API by hand.",
      docLink = "https://spark.apache.org/docs/latest/rdd-programming-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "RDDToDatasetMigration",
      tier = 3,
      description = "Conservatively rewrites an RDD pipeline to the typed Dataset API only when the whole file's RDD usage is guaranteed to compile to the same thing and compute the same result; anything outside that safe surface is logged, not rewritten. See rdd-to-dataset-rewrite-design.md.",
      docLink = "https://spark.apache.org/docs/latest/rdd-programming-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "MultiLineDatasetReadWarn",
      tier = 3,
      description = "Warns that Spark 2.4 and earlier's multi-line text read of Windows line endings (\\r\\n) can leave stray \\r characters; a lineSep of \"\\n\" restores the legacy behavior if it was relied upon.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "MetadataWarnQQ",
      tier = 3,
      description = "Since Spark 3.0, Column.name/Column.as always propagate the underlying NamedExpression's metadata instead of freezing it at call time; restore the old behavior with as(alias, metadata) if relied upon.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "ExecutorPluginWarn",
      tier = 3,
      description = "org.apache.spark.ExecutorPlugin was removed in Spark 3.0 in favor of org.apache.spark.api.plugin.SparkPlugin; a removed API used anywhere is a hard compile break.",
      docLink = "https://spark.apache.org/docs/3.0.0/core-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "AllEquivalentExprs",
      tier = 3,
      description = "EquivalentExpressions.getAllEquivalentExprs was renamed/reshaped to getCommonSubexpressions; rewrites the call site to the new API.",
      docLink = "https://spark.apache.org/docs/latest/core-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "ExpressionEncoder",
      tier = 3,
      description = "ExpressionEncoder.toRow/fromRow were replaced by createSerializer()/createDeserializer(); rewrites call sites to the new API.",
      docLink = "https://spark.apache.org/docs/latest/core-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "MigrateDeprecatedDataFrameReaderFuns",
      tier = 3,
      description = "DataFrameReader.json(RDD[String]) is deprecated; rewrites to session.createDataset(rdd)(Encoders.STRING) passed to the string-based json() overload.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "MigrateHiveContext",
      tier = 3,
      description = "HiveContext is removed; rewrites construction/getOrCreate call sites and the type reference to SparkSession.builder.enableHiveSupport().getOrCreate().sqlContext, and rewrites the plain-HiveContext import case to SQLContext.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "MigrateTrigger",
      tier = 3,
      description = "Structured Streaming's ProcessingTime was removed in favor of Trigger.ProcessingTime/Trigger.Once/Trigger.Continuous; adds the Trigger._ import needed at the call site.",
      docLink = "https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "ScalaTestExtendsFix",
      tier = 3,
      description = "ScalaTest 3.1 renamed FunSuite to AnyFunSuite; rewrites the type reference in an extends clause.",
      docLink = "https://www.scalatest.org/release_notes/3.1.0",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "ScalaTestImportChange",
      tier = 3,
      description = "ScalaTest 3.1 moved several traits (FunSuite family, Matchers) to new packages; rewrites the import/extends-clause call sites to their new names.",
      docLink = "https://www.scalatest.org/release_notes/3.1.0",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "onFailureFix",
      tier = 3,
      description = "Rewrites scala.concurrent.Future#onFailure/onSuccess call sites to onComplete { case Failure(ev) => ... } / onComplete { case Success(sv) => ... }.",
      docLink = "https://docs.scala-lang.org/overviews/core/futures.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal",
      tier = 3,
      description = "Runs the sqlfluff sparksql-upgrade plugin's Family K rule set (13 rules -- CAST, reserved properties, CHAR, format_string, EXTRACT SECOND, percentile_approx, GLOBAL TEMP VIEW, histogram_numeric, lpad/rpad, to_binary/unbase64, CTE precedence, PARTITION literals, TRANSFORM) against a literal SQL string passed to spark.sql(...)/sqlContext.sql(...): auto-rewrites what's fix-compatible, reports the rest as findings. Requires sqlfluff + the plugin on PATH; falls back to no findings if unavailable or if the process fails.",
      docLink = "https://docs.sqlfluff.com/en/stable/dialects.html",
      defaultConfidence = "low"
    ),
    // Tier 2 / Class A config-injection detectors (2026-08-17 review Part 4;
    // gap-analysis 2026-08-21 SS2.2): each one names a
    // spark.sql.legacy.* config in LegacyConfigRegistry that its firing
    // makes eligible for Phase B's submit-conf injection. Detection-only --
    // no code rewrite -- by definition of Tier 2 in the tier taxonomy.
    RuleMeta(
      ruleId = "UntypedScalaUDFDetect",
      tier = 2,
      description = "Flags the deprecated 2-arg functions.udf(AnyRef, DataType) form, which also changed null-handling semantics in Spark 3.0. Feeds spark.sql.legacy.allowUntypedScalaUDF.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "DateTimeFormatPatternValidator",
      tier = 2,
      description = "Flags datetime format pattern strings using letters ('hh' without 'a', or 'F') whose meaning changed under Spark 3.0's DateTimeFormatter. Feeds spark.sql.legacy.timeParserPolicy.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "LooseUpcastDetect",
      tier = 2,
      description = "Flags Dataset.as[T] upcasts to an atomic Scala type, which Spark 3.0 validates more strictly and may reject at analysis time. Feeds spark.sql.legacy.doLooseUpcast.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "PathOptionConflictDetect",
      tier = 2,
      description = "Flags a `path` option coexisting with a path argument to load()/save(), which Spark 3.1 rejects instead of silently picking one. Feeds spark.sql.legacy.pathOptionBehavior.enabled.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "HashOnMapTypeDetect",
      tier = 2,
      description = "Flags hash()/xxhash64() applied directly to a map(...)/create_map(...) result, which Spark 3.0 rejects instead of hashing. Feeds spark.sql.legacy.allowHashOnMapType.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "EmptyCollectionTypeDetect",
      tier = 2,
      description = "Flags no-argument array()/map() calls, which infer NullType element(s) in Spark 3.0 instead of 2.4's StringType. Feeds spark.sql.legacy.createEmptyCollectionUsingStringType.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "MixedIntervalLiteralDetect",
      tier = 2,
      description = "Flags a SQL INTERVAL literal mixing year-month and day-time units, which Spark 3.2+ rejects as a single literal. Feeds spark.sql.legacy.interval.enabled.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "ExponentLiteralDetect",
      tier = 2,
      description = "Flags a scientific-notation numeric literal in SQL text, which parses as Double from Spark 3.0 onward instead of 2.4's Decimal. Feeds spark.sql.legacy.exponentLiteralAsDecimal.enabled.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "GroupingIdTypeDetect",
      tier = 2,
      description = "Flags grouping_id() call sites -- its result is Long from Spark 3.0 onward, not Int. Feeds spark.sql.legacy.integerGroupingId.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "CountStarWithTableIdentDetect",
      tier = 2,
      description = "Flags count(tbl.*) in SQL text, which Spark 3.0's parser rejects -- only bare count(*) is allowed. Feeds spark.sql.legacy.allowStarWithSingleTableIdentifierInCount.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    // Family D/F/G additions (2026-08-22 design review pass). Tier 3 unless
    // otherwise noted -- each of these either has no legacy config at all
    // (Family D/F silent-drift and hard-break items) or would share a config
    // key with an existing Class B entry that the registry can't split
    // (DuplicateMapKeyLiteralDetect). Verified against the real 2.4.8
    // spark-sql/spark-catalyst jars before writing: functions.exists,
    // functions.array_insert, and Dataset.unionByName(_, Boolean) don't exist
    // in 2.4.8 at all (added in 3.0/3.4/3.1 respectively), so no 2.4.8
    // codebase could contain the patterns those design-review items describe
    // -- deliberately not implemented as detectors for that reason, not
    // overlooked.
    RuleMeta(
      ruleId = "AddMonthsSnapDetect",
      tier = 3,
      description = "Flags add_months(...) call sites -- Spark 3.0 no longer reliably snaps the result to the last day of the month for month-end inputs.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "DuplicateMapKeyLiteralDetect",
      tier = 3,
      description = "Flags map(...)/create_map(...) calls with a duplicate literal key, which Spark 3.0 throws on by default instead of silently keeping the last value.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "MapTypeKeyInCreateMapDetect",
      tier = 3,
      description = "Flags a MapType-valued key (a nested map(...)/create_map(...) expression) in map(...)/map_from_arrays(...), which Spark 3.0 disallows at analysis time.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SplitEmptyRegexDetect",
      tier = 3,
      description = "Flags split(col, \"\") with a literal empty-string regex -- Spark 3.4 changed how trailing empty strings are handled in the result array.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "SelfJoinAmbiguousColumnDetect",
      tier = 2,
      description = "Flags df.join(df, ...) self-joins using the exact same DataFrame reference on both sides, which Spark 3.0 can fail at analysis time as ambiguous. Feeds spark.sql.analyzer.failAmbiguousSelfJoin.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "NaFunctionsNameMatchDetect",
      tier = 3,
      description = "Flags na.replace(...)/na.fill(...) call sites -- Spark 3.2 changed DataFrameNaFunctions' column name-matching semantics.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "AnalysisExceptionPlanFieldDetect",
      tier = 3,
      description = "Flags AnalysisException.plan field access -- Spark 3.5 reshapes how a failed analysis's plan is exposed on the caught exception.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    // Family B/C/E/H additions (2026-08-22 design review pass, second round).
    // Verified against the real 2.4.8 jars before writing, same discipline
    // as the Family D/F/G pass above: SparkSession.cloneSession() (Family B)
    // is `private[sql]` in 2.4.8's own source -- no external application code
    // can call it at all, so that design-review item is moot for detecting
    // customer code, not overlooked. SparkContext-in-executor (Family A) and
    // the string<->date binary-comparison cast (Family E) were assessed and
    // deliberately NOT implemented -- both need either closure/lexical-scope
    // tracing or real type inference to avoid false positives/negatives
    // Scalafix's AST-level matching can't provide with confidence. Family J
    // (Hive interop) has no new rule for the same reason RuleRegistry never
    // had one before this pass: it's platform config, already covered as
    // report-only Class C entries in LegacyConfigRegistry, not app code.
    RuleMeta(
      ruleId = "UserDefinedAggregateFunctionDetect",
      tier = 3,
      description = "Flags a class extending UserDefinedAggregateFunction, which is deprecated in Spark 3.0 in favor of Aggregator.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "NegativeDecimalScaleDetect",
      tier = 2,
      description = "Flags DecimalType(precision, scale) constructed with a literal negative scale, which Spark 3.0 rejects by default. Feeds spark.sql.legacy.allowNegativeScaleOfDecimal.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SetCommandSparkConfDetect",
      tier = 2,
      description = "Flags a SQL SET key=value command targeting a non-spark.sql.* config key, which Spark 3.0 rejects by default. Feeds spark.sql.legacy.setCommandRejectsSparkCoreConfs.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "CsvBomMultilineDetect",
      tier = 3,
      description = "Flags a CSV read with multiLine=true and no explicit encoding option -- Spark 3.0 removed automatic BOM detection for CSV.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
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

  // Takes the input explicitly rather than an implicit SemanticDocument so a
  // SyntacticRule (which only ever has a SyntacticDocument in scope -- the
  // two share no common supertype in scalafix.v1) can report findings too.
  def emit(rc: RuleChange, input: Input): Unit = sinkPath.foreach { path =>
    val pos = rc.position
    val line =
      "{" +
        s""""ruleId":"${jsonEscape(rc.ruleId)}",""" +
        s""""file":"${jsonEscape(input.syntax)}",""" +
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
    FindingsSink.emit(rc, doc.input)
    Seq(Patch.lint(rc), rewrite).asPatch
  }

  // For the one SyntacticRule in this jar (ScalaTestExtendsFix) -- same
  // reporting contract, just without requiring semantic (SemanticDB) info.
  def reportSyntactic(rc: RuleChange, rewrite: Patch = Patch.empty)(implicit doc: SyntacticDocument): Patch = {
    FindingsSink.emit(rc, doc.input)
    Seq(Patch.lint(rc), rewrite).asPatch
  }
}
