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
 * Java-side counterpart to the Scalafix {@code AccumulatorUpgrade} rule:
 * {@code SparkContext}/{@code JavaSparkContext}'s v1 accumulator API
 * ({@code accumulator}/{@code intAccumulator}/{@code doubleAccumulator}) is
 * removed in Spark 3.x in favor of the typed accumulator API
 * ({@code longAccumulator}/{@code doubleAccumulator} on {@code SparkContext}
 * itself, or a custom {@code AccumulatorV2}). Verified via {@code javap}
 * against the real {@code spark-core_2.11-2.4.8.jar} (2026-09-24) that every
 * matcher below resolves on {@code JavaSparkContext}.
 *
 * <p>Detect-only, unlike the Scala rule's Tier 1 auto-rewrite: the Scala
 * rule can safely rewrite because it distinguishes the 0L/0.0 zero-initial-
 * value shape (an exact, semantics-preserving substitution) from every other
 * shape (which it neutralizes to compilable-but-manual-review code). Doing
 * the same from a {@code J.MethodInvocation} visitor would need the same
 * literal-argument-shape analysis; not attempted this pass, so every match
 * here is reported for manual review rather than risking a wrong rewrite.
 */
public class SparkAccumulatorV1Detect extends Recipe {

    private static final List<MethodMatcher> MATCHERS = Arrays.asList(
            new MethodMatcher("org.apache.spark.api.java.JavaSparkContext accumulator(..)"),
            new MethodMatcher("org.apache.spark.api.java.JavaSparkContext intAccumulator(..)"),
            new MethodMatcher("org.apache.spark.api.java.JavaSparkContext doubleAccumulator(..)"),
            new MethodMatcher("org.apache.spark.SparkContext accumulator(..)")
    );

    @Override
    public String getDisplayName() {
        return "Detect the removed v1 accumulator API";
    }

    @Override
    public String getDescription() {
        return "SparkContext/JavaSparkContext.accumulator/intAccumulator/doubleAccumulator are removed in Spark " +
                "3.x; migrate to the typed accumulator API (longAccumulator/doubleAccumulator) or a custom AccumulatorV2.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                for (MethodMatcher matcher : MATCHERS) {
                    if (matcher.matches(m)) {
                        return SearchResult.found(
                                m,
                                "The v1 accumulator API is removed in Spark 3.x; migrate to longAccumulator()/" +
                                        "doubleAccumulator() or a custom AccumulatorV2."
                        );
                    }
                }
                return m;
            }
        };
    }
}
