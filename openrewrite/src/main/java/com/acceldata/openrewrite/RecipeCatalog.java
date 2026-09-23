package com.acceldata.openrewrite;

import org.openrewrite.Recipe;
import org.openrewrite.config.Environment;

import com.acceldata.openrewrite.spark.SparkEmptyCollectionTypeDetect;
import com.acceldata.openrewrite.spark.SparkPathOptionConflictDetect;
import com.acceldata.openrewrite.spark.SparkShuffleWriteMetricsRename;
import com.acceldata.openrewrite.spark.SparkUnionAllRename;

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
        byId.put(SparkUnionAllRename.class.getName(), new SparkUnionAllRename());
        byId.put(SparkShuffleWriteMetricsRename.class.getName(), new SparkShuffleWriteMetricsRename());
        byId.put(SparkPathOptionConflictDetect.class.getName(), new SparkPathOptionConflictDetect());
        byId.put(SparkEmptyCollectionTypeDetect.class.getName(), new SparkEmptyCollectionTypeDetect());
        return byId;
    }
}
