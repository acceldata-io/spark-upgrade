package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

import java.util.Arrays;
import java.util.List;

/**
 * Java-side counterpart to the Scalafix {@code MigrateHiveContext} rule:
 * {@code HiveContext} is removed in Spark 3.x; migrate to
 * {@code SparkSession.builder().enableHiveSupport().getOrCreate()}. All
 * three constructors verified present in the real {@code
 * spark-hive_2.11-2.4.8.jar} via {@code javap} (2026-09-24).
 *
 * <p>Detect-only, for the same reason {@code SparkSqlContextConstructorDetect}
 * is: the Java rewrite needs a {@code JavaTemplate} restructuring a
 * constructor call into a builder chain, which needs classpath-resolution
 * setup this pass doesn't have.
 */
public class SparkHiveContextConstructorDetect extends Recipe {

    private static final List<MethodMatcher> MATCHERS = Arrays.asList(
            new MethodMatcher("org.apache.spark.sql.hive.HiveContext <constructor>(org.apache.spark.sql.SparkSession)"),
            new MethodMatcher("org.apache.spark.sql.hive.HiveContext <constructor>(org.apache.spark.SparkContext)"),
            new MethodMatcher("org.apache.spark.sql.hive.HiveContext <constructor>(org.apache.spark.api.java.JavaSparkContext)")
    );

    @Override
    public String getDisplayName() {
        return "Detect direct HiveContext construction";
    }

    @Override
    public String getDescription() {
        return "HiveContext is removed in Spark 3.x; migrate to SparkSession.builder().enableHiveSupport().getOrCreate().";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.NewClass visitNewClass(J.NewClass newClass, ExecutionContext ctx) {
                J.NewClass n = super.visitNewClass(newClass, ctx);
                for (MethodMatcher matcher : MATCHERS) {
                    if (matcher.matches(n)) {
                        return SearchResult.found(
                                n,
                                "HiveContext is removed in Spark 3.x; migrate to " +
                                        "SparkSession.builder().enableHiveSupport().getOrCreate()."
                        );
                    }
                }
                return n;
            }
        };
    }
}
