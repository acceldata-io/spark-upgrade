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
    defaultConfidence: String,
    // True when the rule's rewrite output does not compile against Spark
    // 2.4.8 (it names a 3.x-only symbol). Scala Phase A recompiles SemanticDB
    // against the OLD classpath before every rule, so the CLI runs all
    // targetOnly rules together in ONE final scalafix invocation, after every
    // other Tier 1 rule (TRIAGE-PLAN-2026-10-13 §3.1.1). No two targetOnly
    // rules may rewrite the same span. Only meaningful for tier 1.
    targetOnly: Boolean = false
)

object RuleRegistry {
  private val entries: Seq[RuleMeta] = Seq(
    RuleMeta(
      // Promoted back to Tier 1 (2026-08-22): the rule now handles the
      // three ways this hard break actually shows up in real code --
      // the call site (0L/0.0 constructor forms, named or not), the
      // Accumulator[Long]/[Double] type annotation, and the now-invalid
      // `import org.apache.spark.Accumulator` -- and every case it can't
      // safely resolve (non-zero initial value, Int/other type arg) is
      // reported as AccumulatorUpgrade.ManualMigration (Tier 3) and left as
      // written: it still compiles on 2.4 for the next Phase A rule, and the
      // compile gate names it on 3.5.
      ruleId = "AccumulatorUpgrade",
      tier = 1,
      description = "SparkContext.accumulator is removed in Spark 3.x; migrate to the typed accumulator API (longAccumulator/doubleAccumulator) or a custom AccumulatorV2.",
      docLink = "https://spark.apache.org/docs/latest/core-migration-guide.html#upgrading-from-core-24-to-30",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "AccumulatorUpgrade.ManualMigration",
      tier = 3,
      description = "sc.accumulator / org.apache.spark.Accumulator is removed in Spark 3.x; an Int, non-zero or custom accumulator has no exact typed equivalent and is left for a person.",
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
    // Tier 2: the config, not a rename of "value" references, restores 2.4's
    // column name (probed: groupByKey(identity).count() gives value,count(1)
    // on 2.4.8 and on 3.5.5 with the config; struct keys are unaffected).
    RuleMeta(
      ruleId = "GroupByKeyWarn",
      tier = 2,
      description = "Flags .count()/.agg(...)/.reduceGroups(...) on the result of Dataset.groupByKey. For a non-struct key Spark 3.0 names the grouping column 'key' instead of 2.4's 'value'; spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue is injected and keeps 2.4's 'value'.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "UnionRewrite",
      tier = 1,
      description = "Dataset/DataFrame.unionAll is an alias of union (not deprecated in 2.4 or 3.5.5); renamed to union, the name the docs use. Identical semantics.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSqlExtract",
      tier = 3,
      description = "Plumbing for spark-migrate-cli, not a migration finding: the SQL text of each spark.sql(...)/sqlContext.sql(...) argument -- a literal, a template with its run-time parts as __param__, or dynamic -- which the CLI lints with the SQL linter every language path shares.",
      docLink = "",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SqlInStringDetect",
      tier = 3,
      description = "Flags spark.sql(...)/sqlContext.sql(...) call sites whose SQL is string-interpolated or dynamically built -- the shapes no other SQL rule in this jar can inspect, since they all require a literal argument. A literal argument is not flagged here; it's covered directly by SparkSQLCallExternal and the Scala-regex Family K rules.",
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
      docLink = "https://archive.apache.org/dist/spark/docs/3.0.0/core-migration-guide.html",
      defaultConfidence = "medium"
    ),
    // Stays Tier 3 (2026-10-13 probe): the rule's rewrite,
    // getCommonSubexpressions.map(List(_)), is not 2.4's result. For
    // (a+b)*(a+b) and (a+b)+1, 2.4.8's getAllEquivalentExprs returned every
    // equivalence class, singletons included, each with all its occurrences
    // ([(a+b),(a+b),(a+b)], [(a+b)*(a+b)], [(a+b)+1]); the rewrite on 3.5.5
    // returns only the repeated ones, one occurrence each ([(a+b)]). The 3.x
    // per-class API (getAllExprStates) is private[sql].
    RuleMeta(
      ruleId = "AllEquivalentExprs",
      tier = 3,
      description = "EquivalentExpressions.getAllEquivalentExprs (catalyst internal) was removed in Spark 3.0. getCommonSubexpressions returns only the repeated expressions, one occurrence each, not 2.4's every equivalence class with all its occurrences, so the call site is left for a person.",
      docLink = "https://spark.apache.org/docs/latest/core-migration-guide.html",
      defaultConfidence = "low"
    ),
    // Tier 1 since 2026-10-13, targetOnly: createSerializer/createDeserializer
    // do not exist in 2.4.8. Probed on 2.4.8 and 3.5.5: the same row bytes and
    // decoded values; only 2.4's shared toRow buffer (a row kept without
    // .copy() was overwritten by the next call) is not reproduced.
    RuleMeta(
      ruleId = "ExpressionEncoder",
      // Tier 3, not 1: the rewrite is not exact. 2.4's toRow reused one row
      // buffer per encoder, so a row kept without .copy() was overwritten by
      // the next call; createSerializer() gives each serializer its own buffer
      // (probed 2026-10-13). The compile gate names these lines.
      tier = 3,
      description = "ExpressionEncoder.toRow/fromRow were removed in Spark 3.0: use enc.createSerializer()(x) / enc.createDeserializer()(r). Not rewritten automatically because it is not exact: 2.4's toRow reused one row buffer per encoder, so a row kept without .copy() was overwritten by the next call; with createSerializer() it is not. Check whether the code relied on that.",
      docLink = "https://spark.apache.org/docs/latest/core-migration-guide.html",
      defaultConfidence = "high",
      targetOnly = true
    ),
    RuleMeta(
      ruleId = "ExpressionEncoder.Manual",
      tier = 3,
      description = "ExpressionEncoder.toRow/fromRow were removed in Spark 3.0, and this use (infix or eta-expanded) was not rewritten; call enc.createSerializer()(x) / enc.createDeserializer()(r).",
      docLink = "https://spark.apache.org/docs/latest/core-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "MigrateDeprecatedDataFrameReaderFuns",
      tier = 1,
      description = "DataFrameReader.json(RDD[String]) is deprecated; wraps the RDD argument with <session>.createDataset(rdd)(Encoders.STRING), deriving the actual SparkSession reference from the call site rather than assuming a fixed name. Exact and semantics-preserving.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "MigrateHiveContext",
      tier = 1,
      description = "HiveContext has been deprecated since Spark 2.0 (it still compiles on 3.5.5, with a warning); rewrites construction call sites, the plain-HiveContext import, and bare HiveContext type references to SparkSession.builder.enableHiveSupport().getOrCreate().sqlContext / SQLContext. Exact and semantics-preserving in the construction/import cases (verified against the real 2.4.8 and 3.5.5 jars); the type-reference case carries a narrow residual risk (a Hive-specific method called on the retyped reference), backstopped by the compile gate like every other Tier 1 rewrite.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "MigrateTrigger",
      tier = 1,
      description = "org.apache.spark.sql.streaming.ProcessingTime was removed in favor of Trigger.ProcessingTime/Trigger.Once/Trigger.Continuous; adds the Trigger._ import needed at the call site. Exact and semantics-preserving -- Trigger.ProcessingTime already exists in 2.4.8 too, verified against the real jar.",
      docLink = "https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html",
      defaultConfidence = "high"
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
      tier = 1,
      description = "Rewrites scala.concurrent.Future#onFailure/onSuccess call sites to onComplete { case Failure(ev) => ...; case _ => () } / onComplete { case Success(sv) => ...; case _ => () }. Exact and semantics-preserving -- the trailing catch-all keeps the callback total, matching the original PartialFunction's silent-skip on a non-matching outcome.",
      docLink = "https://docs.scala-lang.org/overviews/core/futures.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal",
      tier = 1,
      description = "Runs the sqlfluff sparksql-upgrade plugin's Family K rule set (17 rules -- CAST, reserved properties, CHAR, format_string, EXTRACT SECOND, percentile_approx, GLOBAL TEMP VIEW, histogram_numeric, lpad/rpad, to_binary/unbase64, CTE precedence, PARTITION literals, TRANSFORM, date/string comparison, CTAS location (data source and Hive), recursive temp view) against a literal SQL string passed to spark.sql(...)/sqlContext.sql(...): auto-rewrites the 3 audited fix codes (FORMATSTRONEINDEX_L004, SPARKSQL_L004, SPARKSQL_L005) only when the whole literal is fixed (a re-lint shows none of them, no new code, no parse error), otherwise reports them as <code>.Manual (Tier 3); reports the other codes as findings. Requires sqlfluff + the plugin on PATH; falls back to no findings if unavailable or if the process fails.",
      docLink = "https://docs.sqlfluff.com/en/stable/dialects.html",
      defaultConfidence = "high"
    ),
    // Tier 2 / Class A config-injection detectors (2026-08-17 review Part 4;
    // gap-analysis 2026-08-21 SS2.2): each one names a
    // spark.sql.legacy.* config in LegacyConfigRegistry that its firing
    // makes eligible for Phase B's submit-conf injection. Detection-only --
    // no code rewrite -- by definition of Tier 2 in the tier taxonomy.
    // Never config-injected: probed, udf((x: Int) => x + 1, IntegerType)
    // on [1, null] gives [2, null] on 2.4.8 but [2, 1] on 3.5.5 with
    // spark.sql.legacy.allowUntypedScalaUDF=true, and that config is job-global.
    // Tier 1 since 2026-10-13 for a lambda with simple annotated parameters
    // and a simple DataType: the typed UDF with a boxed result type was probed
    // equal to 2.4's untyped one (name, type, nullability, values, nulls).
    RuleMeta(
      ruleId = "UntypedScalaUDFDetect",
      tier = 1,
      description = "The untyped functions.udf(f, dataType) form throws at udf creation on Spark 3.x. Where f is a lambda whose parameters are all annotated (primitive, boxed, String, java.sql.Date/Timestamp) and dataType is a simple atomic type, it is rewritten to the typed udf[R, A1, ...](f) with R the boxed result type (udf((x: Int) => x + 1, IntegerType) -> udf[java.lang.Integer, Int](...)): the same column name, type, nullability and values as 2.4, null for a null primitive input included.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "UntypedScalaUDFDetect.Manual",
      tier = 3,
      description = "The untyped functions.udf(AnyRef, DataType) form throws at udf creation on Spark 3.x, and this call is not a lambda with simple annotated parameters and a simple DataType, so it was not rewritten. Rewrite it as a typed UDF (udf[java.lang.Integer, Int]((x: Int) => x + 1)); do not set spark.sql.legacy.allowUntypedScalaUDF, which silently turns a null input into 0 for primitive parameters.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "DateTimeFormatPatternValidator",
      tier = 2,
      description = "Flags datetime format pattern strings Spark 3.0's DateTimeFormatter rejects or reads differently from 2.4's SimpleDateFormat: week-based letters (Y w W u), parsing with E or F, five-letter text forms, letter counts over DateTimeFormatter's limits, the reserved # { } and [ ], F when formatting, and h without a when parsing (the PySpark rule's probed table). Feeds spark.sql.legacy.timeParserPolicy.",
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
    // Tier 1 since 2026-10-13 for one path argument (probed: 2.4.8 used the
    // argument); every other shape is the Tier 2 .Config sub-id.
    RuleMeta(
      ruleId = "PathOptionConflictDetect",
      tier = 1,
      description = "A `path` option coexisting with one path argument to load()/save(), which Spark 3.1 rejects. 2.4 used the argument and ignored the option, so the literal .option(\"path\", v) / the \"path\" pair of a literal options(Map(...)) is removed.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "PathOptionConflictDetect.Config",
      tier = 2,
      description = "A `path` option coexisting with a path argument to load()/save(), which Spark 3.1 rejects, in a shape the tool does not rewrite (several paths, a path option off the call's own chain, a value with possible side effects). Feeds spark.sql.legacy.pathOptionBehavior.enabled.",
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
    // Tier 1 since 2026-10-13 where the generated column name is discarded
    // (probed exact, nullability included); elsewhere the Tier 2 .Config sub-id.
    RuleMeta(
      ruleId = "EmptyCollectionTypeDetect",
      tier = 1,
      description = "No-argument array()/map() infer NullType element(s) in Spark 3.0 instead of 2.4's StringType. Rewritten to array().cast(ArrayType(StringType, false)) / map().cast(MapType(StringType, StringType, false)), 2.4's exact type, where the expression's auto-generated column name is discarded (aliased, withColumn's value, or a filter/where/orderBy/sort/join argument), since the cast changes that name.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "EmptyCollectionTypeDetect.Config",
      tier = 2,
      description = "No-argument array()/map() infer NullType element(s) in Spark 3.0 instead of 2.4's StringType, and the result may become a named output column whose auto-generated name a cast would change, so it was not rewritten. Feeds spark.sql.legacy.createEmptyCollectionUsingStringType.",
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
      description = "Flags count(tbl.*) in SQL text, which Spark 3.0 rejects. In 2.4 it counts rows where every column of tbl is non-null, NOT all rows, so count(*) is not equivalent. spark.sql.legacy.allowStarWithSingleTableIdentifierInCount is injected and restores 2.4 exactly; to drop it, rewrite to count(c1, c2, ...) listing tbl's columns.",
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
    // Tier 1 since 2026-10-13: the rewrite uses only functions 2.4 has and was
    // probed exact on 2.4.8 and 3.5.5 (dates, timestamps, strings, Feb 29,
    // ordinary days, null). A call whose arguments are not safe to repeat is
    // reported as AddMonthsSnapDetect.Manual (Tier 3) and left as written.
    RuleMeta(
      ruleId = "AddMonthsSnapDetect",
      tier = 1,
      description = "Spark 3.0 dropped add_months' month-end snap (2.4: 2019-02-28 + 1 month -> 2019-03-31; 3.x: 2019-03-28). add_months(d, n) is rewritten to when(to_date(d) === last_day(d), last_day(add_months(d, n))).otherwise(add_months(d, n)), which gives 2.4's result on both versions. Applied only where the expression's auto-generated column name is discarded (aliased, withColumn's value, or a filter/where/orderBy/sort/join argument), since the rewrite changes that name.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "AddMonthsSnapDetect.Manual",
      tier = 3,
      description = "Spark 3.0 dropped add_months' month-end snap, and the tool could not rewrite this call safely: its arguments are not simple enough to repeat, or the result may become a named output column whose auto-generated name the rewrite would change. Use when(to_date(d) === last_day(d), last_day(add_months(d, n))).otherwise(add_months(d, n)) to keep 2.4's result (binding complex arguments to vals d and n first), and alias it .as(\"<original name>\") if the column name matters.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "DuplicateMapKeyLiteralDetect",
      tier = 3,
      description = "Flags map(...)/create_map(...) calls with a duplicate literal key, which Spark 3.0 throws on. 2.4 kept both: a lookup returned the first value, collect() the last. No config restores that (mapKeyDedupPolicy=LAST_WIN gives the last everywhere), so remove the duplicate you do not mean.",
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
    // Tier 1 since 2026-10-15: split(s, "(?=)") on 3.5.5 gave 2.4.8's
    // split(s, "") for every input probed, and runs on 2.4 too (not
    // targetOnly). Applied only where the generated column name is discarded.
    RuleMeta(
      ruleId = "SplitEmptyRegexDetect",
      tier = 1,
      description = "Since Spark 3.4, split(col, \"\") no longer ends the result with an empty string (\"abc\" -> [a, b, c], where 2.4 gave [a, b, c, \"\"]); no config restores it. Rewritten to split(col, \"(?=)\"), which gives 2.4's result on both versions. Applied only where the expression's auto-generated column name is discarded (aliased, withColumn's value, or a filter/where/orderBy/sort/join argument), since the rewrite changes that name.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SplitEmptyRegexDetect.Manual",
      tier = 3,
      description = "Since Spark 3.4, split(col, \"\") no longer ends the result with an empty string, and this call was not rewritten because its result may become a named output column whose auto-generated name the rewrite would change. Use split(col, \"(?=)\") to keep 2.4's result, and alias it .as(\"<original name>\") if the column name matters.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
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
      description = "Flags na.replace(...) given column names: since Spark 3.2 a column the frame does not have, or a nested field, throws, where 2.4 skipped it. na.fill is unaffected (probed on 2.4.8 and 3.5.5).",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    // Stays Tier 3 (2026-10-13 probe): reading the plan through
    // ExtendedAnalysisException is not 2.4's result. It matches 2.4.8's
    // plan.isDefined for unresolved columns, type mismatches, bad GROUP BY,
    // union arity, df.col, undefined functions and parse errors, but a missing
    // table (spark.table, SELECT ... FROM, insertInto) has a plan on 3.5.5
    // where 2.4.8 had None.
    RuleMeta(
      ruleId = "AnalysisExceptionPlanFieldDetect",
      tier = 3,
      description = "AnalysisException.plan was removed in Spark 3.x; the plan of a failed analysis is on org.apache.spark.sql.catalyst.ExtendedAnalysisException. Reading it there is not always 2.4's answer: a missing table now carries a plan where 2.4 had None.",
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
    // Ported from pyspark-rules 2026-10-15. Its config is in the CLI's
    // baseline set, so the finding counts as done for the user.
    RuleMeta(
      ruleId = "CommandOutputSchemaDetect",
      tier = 2,
      description = "Flags SHOW DATABASES/SHOW TABLES/DESCRIBE DATABASE in embedded SQL: Spark 3.0 renamed their output columns (databaseName/database -> namespace, database_description_item/value -> info_name/info_value), so code reading the result by the 2.4 name fails. Feeds spark.sql.legacy.keepCommandOutputSchema.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "SetCommandSparkConfDetect",
      tier = 2,
      description = "Flags a SQL SET key=value command targeting a non-spark.sql.* config key, which Spark 3.0 rejects by default. Feeds spark.sql.legacy.setCommandRejectsSparkCoreConfs.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    // 2026-08-24 pass, driven by an item-by-item audit against the upstream
    // SQL and core migration guides (see the coverage doc). Every candidate was
    // checked against the real spark-*_2.11-2.4.8 jars with javap FIRST -- that
    // discipline dropped two rules from this batch before they were written
    // (`Row.json`/`prettyJson` do not exist in 2.4.8 at all, and 2.4.8's
    // `date_add(Column, int)` signature cannot express the argument form the
    // guide says breaks).
    RuleMeta(
      ruleId = "ShuffleWriteMetricsRenameDetect",
      tier = 1,
      description = "ShuffleWriteMetrics.shuffleBytesWritten/shuffleWriteTime/shuffleRecordsWritten were removed in Spark 3.0; renamed to bytesWritten/writeTime/recordsWritten. A pure rename with no semantic change.",
      docLink = "https://spark.apache.org/docs/latest/core-migration-guide.html",
      defaultConfidence = "high"
    ),
    // Tier 1 since 2026-10-13: 2.4.8's isRunningLocally is `return false`.
    RuleMeta(
      ruleId = "IsRunningLocallyWarn",
      tier = 1,
      description = "TaskContext.isRunningLocally() was removed in Spark 3.0 along with local execution; in 2.4.8 it always returned false, so the call is rewritten to false when its receiver (TaskContext.get(), a val or a parameter) has no side effect.",
      docLink = "https://archive.apache.org/dist/spark/docs/3.0.0/core-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "IsRunningLocallyWarn.Manual",
      tier = 3,
      description = "TaskContext.isRunningLocally() was removed in Spark 3.0; it always returned false in 2.4.8. The receiver may have side effects, so the call was not rewritten: replace it with false, keeping the receiver's evaluation if it matters.",
      docLink = "https://archive.apache.org/dist/spark/docs/3.0.0/core-migration-guide.html",
      defaultConfidence = "high"
    ),
    // Tier 1 since 2026-10-13: probed same columns and rows as 2.4.8.
    RuleMeta(
      ruleId = "FromWithoutSelectDetect",
      tier = 1,
      description = "A SQL string that is a bare FROM (`FROM t`, no SELECT) -- accepted by accident in Spark 2.4 as SELECT *, rejected by 3.0's parser -- is rewritten to SELECT * FROM t (probed: the same columns and rows, with WHERE, ORDER BY/LIMIT, JOIN, LATERAL VIEW). Hive's `FROM t SELECT c` still runs and is not flagged.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "FromWithoutSelectDetect.Manual",
      tier = 3,
      description = "A SQL string that is a bare FROM, rejected by Spark 3.0's parser, with GROUP BY, HAVING or WINDOW (where it does not mean SELECT *: 2.4 returned no columns for FROM t GROUP BY c) or a `;`. Add the SELECT list you mean.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "InvalidTimeZoneIdDetect",
      tier = 3,
      description = "Flags a literal timezone ID that java.time cannot resolve (validated exactly as Spark does, via ZoneId.SHORT_IDS); Spark 2.4 silently fell back to GMT, 3.0 throws.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    // Tier 1 since 2026-10-13 for the constructor only, targetOnly (the
    // 3-argument constructor does not exist in 2.4.8); every other reference
    // is the Tier 3 .Manual sub-id.
    RuleMeta(
      ruleId = "CalendarIntervalUsageDetect",
      tier = 1,
      description = "The CalendarInterval(months, microseconds) constructor was removed in Spark 3.0, which added a days field; new CalendarInterval(m, us) is rewritten to new CalendarInterval(m, 0, us), the same interval (probed: the same fields, arithmetic and equality).",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high",
      targetOnly = true
    ),
    RuleMeta(
      ruleId = "CalendarIntervalUsageDetect.Manual",
      tier = 3,
      description = "References to CalendarInterval: Spark 3.0 added a days field and removed the (months, microseconds) constructor, the MICROS_PER_* constants, the from*String parsers, milliseconds(), add, subtract and negate, so code calling those no longer compiles and code reading microseconds silently gets different numbers.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    // New 2026-10-13, Tier 1, targetOnly: OneHotEncoder is the old
    // Transformer in 2.4.8. javap and a fit/transform probe: the 3.5.5
    // OneHotEncoder is 2.4.8's OneHotEncoderEstimator plus single-column params.
    RuleMeta(
      ruleId = "OneHotEncoderEstimatorRename",
      tier = 1,
      description = "OneHotEncoderEstimator was renamed to OneHotEncoder in Spark 3.0 (the old OneHotEncoder Transformer was removed); every reference is renamed. The 3.x OneHotEncoder has the same parameters, setters and OneHotEncoderModel (probed: the same output).",
      docLink = "https://spark.apache.org/docs/latest/ml-migration-guide.html",
      defaultConfidence = "high",
      targetOnly = true
    ),
    RuleMeta(
      ruleId = "OneHotEncoderEstimatorRename.Manual",
      tier = 3,
      description = "OneHotEncoderEstimator was renamed to OneHotEncoder in Spark 3.0, but this file also uses 2.4's old OneHotEncoder Transformer, which was removed; migrate that code, then rename OneHotEncoderEstimator to OneHotEncoder.",
      docLink = "https://spark.apache.org/docs/latest/ml-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "JsonEmptyStringDetect",
      tier = 2,
      description = "Flags a JSON read with an explicit schema whose mode makes a malformed record visible -- FAILFAST, DROPMALFORMED, a corrupt-record column, or a mode the rule cannot read: from Spark 3.0 an empty string in a non-string field makes the record malformed (the job fails, the row is dropped, or the column fills), where 2.4 read it as null. In the default PERMISSIVE mode 3.5.5 gives 2.4's row (probed), so that read is not flagged. Feeds spark.sql.legacy.json.allowEmptyString.enabled.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "DateTimeStringComparisonDetect",
      tier = 2,
      description = "Flags a Column comparison with a date-shaped string literal where a probe showed the answer can differ: Spark 3.0 casts the string to date/timestamp, where 2.4 cast the date/timestamp to string. ts > \"2020-01-01\" kept midnight rows on 2.4 and drops them on 3.x; d === \"2020-1-1\" matched nothing on 2.4. A zero-padded date under ===, =!=, <, >= is not flagged (same answer). Feeds spark.sql.legacy.typeCoercion.datetimeToString.enabled.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "SaveAsTablePathDetect",
      tier = 2,
      description = "Flags saveAsTable with a `path` option in ErrorIfExists (default) mode; from Spark 3.2 it fails when the location is a non-empty directory, where 2.4 overwrote it. Feeds spark.sql.legacy.allowNonEmptyLocationInCTAS.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "StoreAssignmentPolicyDetect",
      tier = 2,
      description = "Flags a write into an existing table -- SQL INSERT INTO/OVERWRITE, insertInto, saveAsTable in append mode. From Spark 3.0 (storeAssignmentPolicy=ANSI) an unsafe type conversion, such as string to int or date, fails the write instead of casting, and an overflow fails at run time. Feeds spark.sql.storeAssignmentPolicy.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "CsvBinaryTypeDetect",
      tier = 3,
      description = "Flags a CSV read whose explicit schema declares BinaryType, which Spark 3.4 no longer supports for CSV. No legacy config exists.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    // The sqlfluff plugin's 13 Family K codes, split by whether `rules.py`
    // implements a `LintFix` for them (2026-08-26 tier promotion). The 6
    // fixable codes are now real, correctly-attributed Tier 1 rewrites --
    // lint runs against the original SQL before `fix` touches it, so each
    // code is captured from the actual violation, not lost/misattributed
    // the way the old fix-then-lint order did. The 7 detect-only codes get
    // their own Tier 3 entry so they DON'T inherit the parent's new Tier 1
    // through the sub-rule fallback in `apply` below -- they have no fix,
    // so auto-applying them would be wrong. CTEPRECEDENCE_L010 keeps its
    // Tier 2: no fix exists for it, and it's the one code Phase B's
    // config-injection actually keys on (spark.sql.legacy.ctePrecedencePolicy).
    RuleMeta(
      ruleId = "SparkSQLCallExternal.SPARKSQLCAST_L001",
      tier = 3,
      description = "sqlfluff plugin: cast(x as int) on a string fails under ANSI mode (off by default in 3.5). Detect-only: the plugin's int(x) rewrite is the same expression, so it is not applied.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.RESERVEDROPERTIES_L002",
      tier = 2,
      description = "sqlfluff plugin: a reserved table/database property (provider/location/owner, plus external from 3.2) set via TBLPROPERTIES/DBPROPERTIES. Spark 3.0 rejects it; 2.4 stored it as an ordinary property, which spark.sql.legacy.notReserveProperties restores. Not rewritten: moving it into USING/LOCATION changes the table.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.NOCHARS_L003",
      tier = 2,
      description = "sqlfluff plugin: a CHAR(n) column. 2.4 made it a STRING; from 3.1 it is a padded CHAR, so equality against a STRING silently stops matching. spark.sql.legacy.charVarcharAsString restores 2.4's STRING. Not rewritten: the plugin's rewrite also changes Hive-format DDL.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.FORMATSTRONEINDEX_L004",
      tier = 1,
      description = "sqlfluff plugin: format_string's %0$ zero-indexed form (silently treated as %1$ under 2.4/JDK8) is rewritten to %1$ where the auto-generated column name is discarded (aliased, or outside the SELECT list).",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.SPARKSQL_L004",
      tier = 1,
      description = "sqlfluff plugin: extract(second from ts) changed return type from IntegerType (2.4) to DecimalType(8,6) (3.0+); rewritten to second(ts), which is what 2.4 parsed it into: the same value, type and column name.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    // The three audited fix codes are Tier 1 only where the literal was
    // rewritten whole. A site the plugin offers no fix for (an unaliased
    // SELECT item, whose auto-generated column name the fix would change), or
    // a literal that could not be fixed whole, is reported under
    // `<code>.Manual` at Tier 3 and left as written.
    RuleMeta(
      ruleId = "SparkSQLCallExternal.FORMATSTRONEINDEX_L004.Manual",
      tier = 3,
      description = "sqlfluff plugin: format_string's %0$ zero-indexed form (read as %1$ by 2.4/JDK 8, rejected on JDK 17) was not rewritten, because %1$ would change an unaliased SELECT item's auto-generated column name (or the literal could not be fixed whole). Alias it with its 2.4 name and use %1$.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.SPARKSQL_L004.Manual",
      tier = 3,
      description = "sqlfluff plugin: extract(second from ts) returns DecimalType(8,6) from Spark 3.0 (2.4: an int); this literal was not rewritten to second(ts) because it could not be fixed whole. Apply second(ts) by hand.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.SPARKSQL_L005.Manual",
      tier = 3,
      description = "sqlfluff plugin: approx_percentile's accuracy must be integral from Spark 3.0 (2.4 coerced a fractional one); not rewritten, because int(accuracy) would change an unaliased SELECT item's auto-generated column name (or the literal could not be fixed whole). Alias it with its 2.4 name and wrap the accuracy in int(...).",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.SPARKSQL_L005",
      tier = 1,
      description = "sqlfluff plugin: approx_percentile's accuracy argument must be an exact integral type from Spark 3.0 (2.4 silently coerced a fractional one); rewritten to int(...) where the auto-generated column name is discarded (aliased, or outside the SELECT list).",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.GLOBALTEMPVIEW_L006",
      tier = 3,
      description = "sqlfluff plugin: global temp views aren't portable across all Spark runtimes (Spark Connect / serverless). Detect-only, no safe rewrite.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.HISTOGRAMNUMERIC_L007",
      tier = 3,
      description = "sqlfluff plugin: histogram_numeric's (x, y) struct field x matches the input's numeric type from Spark 3.3, was always DoubleType before. Detect-only.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.BYTEPADDING_L008",
      tier = 3,
      description = "sqlfluff plugin: lpad/rpad gained a BINARY overload in Spark 3.3 with byte-sequence, not character, padding semantics. Detect-only.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.BINARYCONV_L009",
      tier = 3,
      description = "sqlfluff plugin: to_binary now throws on malformed input (Spark 3.3) instead of returning NULL. Detect-only.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.CTEPRECEDENCE_L010",
      tier = 2,
      description = "sqlfluff plugin: an inner WITH shadows an outer CTE of the same name. 2.4 used the OUTER definition; Spark 3.x fails the query by default. Feeds spark.sql.legacy.ctePrecedencePolicy=LEGACY, which restores 2.4's outer-wins resolution (CORRECTED would use the inner one and change the result).",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.TYPEDPARTITION_L011",
      tier = 3,
      description = "sqlfluff plugin: untyped partition literals are handled more strictly from Spark 3.0. Detect-only.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.TRANSFORMDELIM_L012",
      tier = 3,
      description = "sqlfluff plugin: TRANSFORM's default delimiters and no-AS schema inference changed in Spark 3.0. Detect-only.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "low"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.DATESTRCOMPARE_L013",
      tier = 2,
      description = "sqlfluff plugin: a date-shaped string literal compared where a probe showed the answer can differ. Spark 3.0 casts the string to the other side's date or timestamp type, 2.4 cast the date or timestamp to a string: ts > '2020-01-01' kept midnight rows on 2.4 and drops them on 3.x; d = '2020-1-1' matched nothing on 2.4. Feeds spark.sql.legacy.typeCoercion.datetimeToString.enabled.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.CTASLOCATION_L014",
      tier = 2,
      description = "sqlfluff plugin: CREATE TABLE ... USING ... LOCATION (or OPTIONS path) ... AS SELECT. From Spark 3.2 it fails when the location already holds files; 2.4 replaced them. Feeds spark.sql.legacy.allowNonEmptyLocationInCTAS.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.HIVECTASLOCATION_L015",
      tier = 3,
      description = "sqlfluff plugin: a Hive-format CREATE TABLE ... LOCATION ... AS SELECT (no USING). From Spark 3.2 it fails when the location already holds files; 2.4 replaced them. No config restores 2.4 here; delete the location's files before the CTAS.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.RECURSIVEVIEW_L016",
      tier = 2,
      description = "sqlfluff plugin: a temp view defined in terms of its previous definition (CREATE OR REPLACE TEMP VIEW v AS ... FROM v). 2.4 used the old view; Spark 3.1+ fails with RECURSIVE_VIEW. Feeds spark.sql.legacy.storeAnalyzedPlanForView.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "high"
    ),
    RuleMeta(
      ruleId = "SparkSQLCallExternal.UNPARSEABLE",
      tier = 3,
      description = "sqlfluff's sparksql dialect could not parse a literal SQL string, so none of the Family K SQL rules were evaluated against it -- the absence of SQL findings for that literal means 'not checked', not 'clean'.",
      docLink = "https://docs.sqlfluff.com/en/stable/dialects.html",
      defaultConfidence = "medium"
    ),
    RuleMeta(
      ruleId = "CsvBomMultilineDetect",
      tier = 3,
      description = "Flags a CSV read with multiLine=true and no explicit encoding option. A UTF-8 BOM is still skipped in Spark 3.x, but a UTF-16/UTF-32 file is no longer detected from its BOM and is misread; set the encoding option explicitly.",
      docLink = "https://spark.apache.org/docs/latest/sql-migration-guide.html",
      defaultConfidence = "medium"
    ),
    // Not a Spark API change -- a Scala 2.11 -> 2.12 SOURCE break (code
    // review 2026-08-22 §8.2). The tool bumps scalaVersion to 2.12 (Tier 1)
    // but had nothing for the source-level fallout of that bump until this
    // rule; a purely syntactic, exact rewrite (insert ": Unit =" before an
    // existing body, nothing else touched), so it earns Tier 1 the same way
    // MigrateHiveContext/MigrateTrigger/onFailureFix did in the 2026-08-25
    // tier audit -- and being compile-breaking, any mistake here is caught
    // immediately by the compile gate.
    RuleMeta(
      ruleId = "ProcedureSyntaxDetect",
      tier = 1,
      description = "Procedure syntax (def f() { ... }, no `=`) still compiles on Scala 2.12 but is deprecated in 2.13 and dropped in Scala 3; rewrites to def f(): Unit = { ... }.",
      docLink = "https://docs.scala-lang.org/scala3/guides/migration/incompat-syntactic.html#procedure-syntax",
      defaultConfidence = "high"
    ),
    // New 2026-10-13: source that compiles on 2.4.8/2.11/JDK 8 and not on
    // 3.5.5/2.12/JDK 17, each probed both ways. Every finding is a
    // `CompileBreakDetect.<Sub>` sub-id with its own text. Tier 1, targetOnly
    // since 2026-10-14 for the rows with an exact rewrite (they inherit this
    // entry); the rows without one and every `.Manual` site are Tier 3 below.
    RuleMeta(
      ruleId = "CompileBreakDetect",
      tier = 1,
      description = "Code that compiles on Spark 2.4.8 / Scala 2.11 / JDK 8 but not on Spark 3.5.5 / Scala 2.12 / JDK 17, rewritten where the 3.5.5 form is exact: an untyped foreachPartition/foreachBatch lambda gets its inferred types, RowEncoder(schema) becomes ExpressionEncoder(schema), removed MLlib aliases become their equivalents, the window boundary functions become Window's Long boundaries, databricks .avro(path) becomes format(\"avro\"), InterfaceStability.X becomes annotation.X.",
      docLink = "https://spark.apache.org/docs/latest/core-migration-guide.html",
      defaultConfidence = "high",
      targetOnly = true
    )
  ) ++
    // No exact rewrite (sun.misc in Scala: no SemanticDB, analysis compiles on JDK 17).
    Seq("SunMiscBase64", "MlKMeansComputeCost", "SparkHadoopUtil", "ImageSchemaReadImages", "AccumulableApi",
      "MllibWithSgd").map(sub => RuleMeta(
      ruleId = s"CompileBreakDetect.$sub",
      tier = 3,
      description = "Code that compiles on Spark 2.4.8 / Scala 2.11 / JDK 8 but not on Spark 3.5.5 / Scala 2.12 / JDK 17, with no exact rewrite (a removed API, sun.misc.BASE64*). The finding names the fix.",
      docLink = "https://spark.apache.org/docs/latest/core-migration-guide.html",
      defaultConfidence = "high"
    )) ++
    // A site of a rewriting row in a shape the rewrite does not handle.
    Seq("ForeachPartitionLambda", "ForeachBatchLambda", "MulticlassMetricsNoLabel", "KMeansRuns",
      "GbtNumTrees", "StageInfoAttemptId", "InterfaceStability", "MlReadWriteContext", "WindowBoundaryFunctions",
      "RangeBetweenColumns", "DatabricksAvro", "MlpLayers").map(sub => RuleMeta(
      ruleId = s"CompileBreakDetect.$sub.Manual",
      tier = 3,
      description = "Code that compiles on Spark 2.4.8 but not on Spark 3.5.5, in a shape the tool does not rewrite exactly (a placeholder lambda, a lit(n) window bound, KMeans.getRuns, ...). The finding says why and names the fix.",
      docLink = "https://spark.apache.org/docs/latest/core-migration-guide.html",
      defaultConfidence = "high"
    ))

  val all: Map[String, RuleMeta] = entries.map(m => m.ruleId -> m).toMap

  /** Falls back to a Tier 3 "undocumented" entry rather than throwing, since a
   *  rule firing with no registry entry is a rule-authoring bug, not something
   *  that should crash the whole analysis run.
   *
   *  One level of `Rule.Sub` qualification is resolved before that fallback:
   *  `SparkSQLCallExternal` emits `SparkSQLCallExternal.<PLUGIN_CODE>` so the
   *  sqlfluff plugin's 13 rules are individually addressable. The handful of
   *  codes that need their own tier (because they map to a legacy config) get
   *  their own entry above; the rest should inherit the parent rule's metadata
   *  rather than silently degrade to "Undocumented rule". */
  def apply(ruleId: String): RuleMeta =
    all
      .get(ruleId)
      .orElse(ruleId.lastIndexOf('.') match {
        case -1 => None
        case i => all.get(ruleId.substring(0, i)).map(_.copy(ruleId = ruleId))
      })
      .getOrElse(RuleMeta(ruleId, tier = 3, description = "Undocumented rule.", docLink = "", defaultConfidence = "low"))
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
