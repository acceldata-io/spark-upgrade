package com.acceldata.openrewrite;

import org.openrewrite.Recipe;
import org.openrewrite.config.Environment;

import com.acceldata.openrewrite.spark.SparkAccumulableInfoConstructionDetect;
import com.acceldata.openrewrite.spark.SparkAccumulatorV1Detect;
import com.acceldata.openrewrite.spark.SparkAddMonthsRewrite;
import com.acceldata.openrewrite.spark.SparkAddMonthsUsageWarn;
import com.acceldata.openrewrite.spark.SparkAnalysisExceptionPlanUsageDetect;
import com.acceldata.openrewrite.spark.SparkCalendarIntervalUsageDetect;
import com.acceldata.openrewrite.spark.SparkCommandOutputSchemaDetect;
import com.acceldata.openrewrite.spark.SparkCompileBreakDetect;
import com.acceldata.openrewrite.spark.SparkCompileBreakRewrite;
import com.acceldata.openrewrite.spark.SparkDateTimeFormatPatternDetect;
import com.acceldata.openrewrite.spark.SparkDateTimeStringComparisonDetect;
import com.acceldata.openrewrite.spark.SparkDuplicateMapKeyLiteralDetect;
import com.acceldata.openrewrite.spark.SparkEmptyCollectionTypeDetect;
import com.acceldata.openrewrite.spark.SparkExecutorPluginDetect;
import com.acceldata.openrewrite.spark.SparkGroupByKeyCountWarn;
import com.acceldata.openrewrite.spark.SparkHashOnMapTypeDetect;
import com.acceldata.openrewrite.spark.SparkHiveContextConstructorDetect;
import com.acceldata.openrewrite.spark.SparkInvalidTimeZoneIdDetect;
import com.acceldata.openrewrite.spark.SparkIsRunningLocallyDetect;
import com.acceldata.openrewrite.spark.SparkJsonEmptyStringDetect;
import com.acceldata.openrewrite.spark.SparkLog4j1UsageDetect;
import com.acceldata.openrewrite.spark.SparkMapTypeKeyInCreateMapDetect;
import com.acceldata.openrewrite.spark.SparkMesosUsageDetect;
import com.acceldata.openrewrite.spark.SparkMllibSgdModelUsageDetect;
import com.acceldata.openrewrite.spark.SparkNaFunctionsReplaceDetect;
import com.acceldata.openrewrite.spark.SparkNegativeDecimalScaleDetect;
import com.acceldata.openrewrite.spark.SparkOneHotEncoderEstimatorUsageDetect;
import com.acceldata.openrewrite.spark.SparkPathOptionConflictDetect;
import com.acceldata.openrewrite.spark.SparkProcessingTimeDetect;
import com.acceldata.openrewrite.spark.SparkSaveAsTablePathDetect;
import com.acceldata.openrewrite.spark.SparkSelfJoinAmbiguousDetect;
import com.acceldata.openrewrite.spark.SparkShuffleWriteMetricsRename;
import com.acceldata.openrewrite.spark.SparkSqlContextConstructorDetect;
import com.acceldata.openrewrite.spark.SparkSqlLiteralExtract;
import com.acceldata.openrewrite.spark.SparkStoreAssignmentPolicyDetect;
import com.acceldata.openrewrite.spark.SparkUnionAllRename;
import com.acceldata.openrewrite.spark.SparkIsRunningLocallyRewrite;
import com.acceldata.openrewrite.spark.SparkEmptyCollectionTypeRewrite;
import com.acceldata.openrewrite.spark.SparkPathOptionConflictRewrite;
import com.acceldata.openrewrite.spark.SparkOneHotEncoderEstimatorRename;
import com.acceldata.openrewrite.spark.SparkCalendarIntervalConstructorRewrite;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves a recipe-id list (or {@code "all"}) to real {@link Recipe}
 * instances -- our four hand-written recipes via direct {@code new}, plus
 * the vendored JDK-migration bundle via {@link Environment#scanRuntimeClasspath()}
 * + {@code activateRecipes(...)} (spark-migrate-java-openrewrite-integration-
 * 2026-09-18.md SS8.3's decision to bundle the JDK 8/11->17 axis alongside the
 * Spark axis this iteration; the resolution path itself confirmed against a
 * real spike, not assumed from docs -- resolving cleanly as a plain
 * dependency, no {@code rewrite-maven-plugin} needed).
 *
 * <p>Recipe ids are the fully-qualified class name for our own recipes (the
 * same convention OpenRewrite itself uses for Java-authored recipes) and
 * OpenRewrite's own id verbatim for the vendored one -- {@code
 * JavaRuleRegistry} on the spark-migrate-cli side keys off these exact
 * strings, the same way {@code PyRuleRegistry} keys off PySparkler's own
 * {@code transformer_id}s verbatim.
 */
public final class RecipeCatalog {

    public static final String JDK_17_MIGRATION_ID = "org.openrewrite.java.migrate.UpgradeToJava17";

    private RecipeCatalog() {
    }

    /** All hand-written recipe ids this jar owns (excludes the vendored JDK
     * bundle, which is resolved separately via {@link Environment}). */
    public static List<String> handWrittenRecipeIds() {
        return new ArrayList<>(handWritten().keySet());
    }

    public static List<String> allRecipeIds() {
        List<String> ids = handWrittenRecipeIds();
        ids.add(JDK_17_MIGRATION_ID);
        return ids;
    }

    /** Resolves a comma-separated id list, or the literal {@code "all"}, to
     * real Recipe instances. Unknown ids are skipped with a message on
     * stderr rather than failing the whole run -- a typo in one requested id
     * shouldn't block every other recipe from running. */
    public static List<Recipe> resolve(String recipeIdsArg) {
        List<String> requested = "all".equals(recipeIdsArg)
                ? allRecipeIds()
                : List.of(recipeIdsArg.split(","));

        Map<String, Recipe> handWritten = handWritten();
        Environment environment = Environment.builder().scanRuntimeClasspath().build();

        List<Recipe> resolved = new ArrayList<>();
        for (String id : requested) {
            String trimmed = id.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (handWritten.containsKey(trimmed)) {
                resolved.add(handWritten.get(trimmed));
            } else {
                try {
                    resolved.add(environment.activateRecipes(trimmed));
                } catch (RuntimeException e) {
                    System.err.println("[RecipeCatalog] Unknown recipe id, skipping: " + trimmed + " (" + e.getMessage() + ")");
                }
            }
        }
        return resolved;
    }

    private static Map<String, Recipe> handWritten() {
        Map<String, Recipe> byId = new LinkedHashMap<>();
        // Tier 1 (2026-09-24): exact, type-attribution-safe renames, wired
        // into codegen's fix mode.
        byId.put(SparkUnionAllRename.class.getName(), new SparkUnionAllRename());
        byId.put(SparkShuffleWriteMetricsRename.class.getName(), new SparkShuffleWriteMetricsRename());
        // Everything else: Tier 3, detect-only (JavaRuleRegistry is the
        // authority on tier; this list is just "what exists").
        byId.put(SparkPathOptionConflictDetect.class.getName(), new SparkPathOptionConflictDetect());
        byId.put(SparkEmptyCollectionTypeDetect.class.getName(), new SparkEmptyCollectionTypeDetect());
        byId.put(SparkAccumulatorV1Detect.class.getName(), new SparkAccumulatorV1Detect());
        byId.put(SparkSqlContextConstructorDetect.class.getName(), new SparkSqlContextConstructorDetect());
        byId.put(SparkHiveContextConstructorDetect.class.getName(), new SparkHiveContextConstructorDetect());
        byId.put(SparkProcessingTimeDetect.class.getName(), new SparkProcessingTimeDetect());
        byId.put(SparkGroupByKeyCountWarn.class.getName(), new SparkGroupByKeyCountWarn());
        byId.put(SparkHashOnMapTypeDetect.class.getName(), new SparkHashOnMapTypeDetect());
        byId.put(SparkSelfJoinAmbiguousDetect.class.getName(), new SparkSelfJoinAmbiguousDetect());
        byId.put(SparkNegativeDecimalScaleDetect.class.getName(), new SparkNegativeDecimalScaleDetect());
        byId.put(SparkExecutorPluginDetect.class.getName(), new SparkExecutorPluginDetect());
        byId.put(SparkIsRunningLocallyDetect.class.getName(), new SparkIsRunningLocallyDetect());
        byId.put(SparkJsonEmptyStringDetect.class.getName(), new SparkJsonEmptyStringDetect());
        byId.put(SparkStoreAssignmentPolicyDetect.class.getName(), new SparkStoreAssignmentPolicyDetect());
        byId.put(SparkCommandOutputSchemaDetect.class.getName(), new SparkCommandOutputSchemaDetect());
        byId.put(SparkDateTimeStringComparisonDetect.class.getName(), new SparkDateTimeStringComparisonDetect());
        byId.put(SparkSaveAsTablePathDetect.class.getName(), new SparkSaveAsTablePathDetect());
        byId.put(SparkCalendarIntervalUsageDetect.class.getName(), new SparkCalendarIntervalUsageDetect());
        byId.put(SparkInvalidTimeZoneIdDetect.class.getName(), new SparkInvalidTimeZoneIdDetect());
        // 2026-09-29 coverage review batch -- all Tier 3, detect-only, per
        // this session's policy for every new rule (see each class's own
        // doc comment for the migration-guide item and javap verification).
        byId.put(SparkAccumulableInfoConstructionDetect.class.getName(), new SparkAccumulableInfoConstructionDetect());
        byId.put(SparkAnalysisExceptionPlanUsageDetect.class.getName(), new SparkAnalysisExceptionPlanUsageDetect());
        byId.put(SparkNaFunctionsReplaceDetect.class.getName(), new SparkNaFunctionsReplaceDetect());
        byId.put(SparkDuplicateMapKeyLiteralDetect.class.getName(), new SparkDuplicateMapKeyLiteralDetect());
        byId.put(SparkMapTypeKeyInCreateMapDetect.class.getName(), new SparkMapTypeKeyInCreateMapDetect());
        byId.put(SparkAddMonthsUsageWarn.class.getName(), new SparkAddMonthsUsageWarn());
        // 2026-10-13: the exact rewrite for the add_months calls it can prove
        // safe; SparkAddMonthsUsageWarn reports the rest.
        byId.put(SparkAddMonthsRewrite.class.getName(), new SparkAddMonthsRewrite());
        // 2026-10-13 triage: exact rewrites for the sites each one can prove,
        // split from a detect recipe that reports the rest (except the rename,
        // whose detect reports only an import whose type did not resolve).
        byId.put(SparkIsRunningLocallyRewrite.class.getName(), new SparkIsRunningLocallyRewrite());
        byId.put(SparkEmptyCollectionTypeRewrite.class.getName(), new SparkEmptyCollectionTypeRewrite());
        byId.put(SparkPathOptionConflictRewrite.class.getName(), new SparkPathOptionConflictRewrite());
        byId.put(SparkOneHotEncoderEstimatorRename.class.getName(), new SparkOneHotEncoderEstimatorRename());
        byId.put(SparkCalendarIntervalConstructorRewrite.class.getName(), new SparkCalendarIntervalConstructorRewrite());
        byId.put(SparkDateTimeFormatPatternDetect.class.getName(), new SparkDateTimeFormatPatternDetect());
        byId.put(SparkMesosUsageDetect.class.getName(), new SparkMesosUsageDetect());
        byId.put(SparkLog4j1UsageDetect.class.getName(), new SparkLog4j1UsageDetect());
        byId.put(SparkOneHotEncoderEstimatorUsageDetect.class.getName(), new SparkOneHotEncoderEstimatorUsageDetect());
        byId.put(SparkMllibSgdModelUsageDetect.class.getName(), new SparkMllibSgdModelUsageDetect());
        // 2026-10-13: Spark 3.5.5 / JDK 17 compile breaks, one recipe per
        // table row, each under its own SparkCompileBreakDetect.<Sub> id.
        for (SparkCompileBreakDetect recipe : SparkCompileBreakDetect.all()) {
            byId.put(recipe.getName(), recipe);
        }
        // 2026-10-14: the exact 3.5.5 form of the rows that have one
        // (SparkCompileBreakRewrite.<Sub>); the detect rows report the rest.
        for (SparkCompileBreakRewrite recipe : SparkCompileBreakRewrite.all()) {
            byId.put(recipe.getName(), recipe);
        }
        // Plumbing, not a migration finding of its own -- see its own doc
        // comment. JavaRuleRegistry does not carry an entry for this id;
        // JavaAnalysisRunner filters it out of the findings stream before
        // JavaRuleRegistry ever sees it.
        byId.put(SparkSqlLiteralExtract.class.getName(), new SparkSqlLiteralExtract());
        return byId;
    }
}
