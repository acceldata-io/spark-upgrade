package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.marker.SearchResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Java-side counterpart to the Scalafix {@code AccumulatorUpgrade} rule:
 * {@code SparkContext}/{@code JavaSparkContext}'s v1 accumulator API
 * ({@code accumulator}/{@code intAccumulator}/{@code doubleAccumulator}) is
 * removed in Spark 3.x in favor of the typed accumulator API
 * ({@code longAccumulator}/{@code doubleAccumulator} on {@code SparkContext}
 * itself, or a custom {@code AccumulatorV2}). Verified via {@code javap}
 * against the real {@code spark-core_2.11-2.4.8.jar} that every
 * matcher below resolves on {@code JavaSparkContext}.
 *
 * <p>Reports the calls {@link SparkAccumulatorV1Rewrite} does not rewrite: a
 * non-zero initial value, a variable used other than through add/value, a
 * call that does not initialise a declaration.
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
            private Map<JavaType.Variable, String> rewritten = Collections.emptyMap();

            @Override
            public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
                rewritten = SparkAccumulatorV1Rewrite.rewritten(cu);
                return super.visitCompilationUnit(cu, ctx);
            }

            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                boolean handled = SparkAccumulatorV1Rewrite.handles(method, rewritten, getCursor());
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (handled) {
                    return m;
                }
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
