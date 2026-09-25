package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code MigrateToSparkSessionBuilder}
 * rule: direct {@code SQLContext} construction is deprecated in favor of
 * {@code SparkSession.builder()...getOrCreate()}. All three constructors
 * verified present in the real {@code spark-sql_2.11-2.4.8.jar} via
 * {@code javap} (2026-09-24).
 *
 * <p>Detect-only, unlike the Scala rule's Tier 1 rewrite: the Scala rule
 * rewrites a construction expression in place because Scala's `new
 * SQLContext(sc)` and `SparkSession.builder().sparkContext(sc)...` are both
 * single expressions substitutable at the same call site. The equivalent
 * Java rewrite needs a {@code JavaTemplate} to restructure the call shape
 * (`new SQLContext(sc)` &#8594; a builder chain), which needs its own
 * classpath-resolution setup this pass doesn't have -- flagged for manual
 * migration instead of risking a wrong auto-rewrite.
 */
public class SparkSqlContextConstructorDetect extends Recipe {

    private static final MethodMatcher SPARK_SESSION_CTOR =
            new MethodMatcher("org.apache.spark.sql.SQLContext <constructor>(org.apache.spark.sql.SparkSession)");
    private static final MethodMatcher SPARK_CONTEXT_CTOR =
            new MethodMatcher("org.apache.spark.sql.SQLContext <constructor>(org.apache.spark.SparkContext)");
    private static final MethodMatcher JAVA_SPARK_CONTEXT_CTOR =
            new MethodMatcher("org.apache.spark.sql.SQLContext <constructor>(org.apache.spark.api.java.JavaSparkContext)");

    @Override
    public String getDisplayName() {
        return "Detect direct SQLContext construction";
    }

    @Override
    public String getDescription() {
        return "new SQLContext(...) is deprecated; migrate to SparkSession.builder()...getOrCreate().";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.NewClass visitNewClass(J.NewClass newClass, ExecutionContext ctx) {
                J.NewClass n = super.visitNewClass(newClass, ctx);
                if (SPARK_SESSION_CTOR.matches(n) || SPARK_CONTEXT_CTOR.matches(n) || JAVA_SPARK_CONTEXT_CTOR.matches(n)) {
                    return SearchResult.found(
                            n,
                            "Direct SQLContext construction is deprecated; migrate to " +
                                    "SparkSession.builder()...getOrCreate().sqlContext()."
                    );
                }
                return n;
            }
        };
    }
}
