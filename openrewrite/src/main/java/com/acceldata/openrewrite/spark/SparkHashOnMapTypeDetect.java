package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code HashOnMapTypeDetect} rule:
 * {@code hash()} applied to a {@code map(...)} result throws in Spark 3.0
 * instead of silently hashing. Narrowed exactly like the Scala rule to the
 * unambiguous, zero-false-positive case: the argument is itself a direct
 * {@code functions.map(...)} call. Feeds {@code
 * spark.sql.legacy.allowHashOnMapType} -- Tier 2 as of 2026-09-25, cross-linked
 * into the existing {@code LegacyConfigRegistry} entry's {@code detectionRuleIds}
 * so Phase B (spark-migrate-cli's {@code PhaseBRunner}) actually injects it.
 *
 * <p>{@code xxhash64} is deliberately NOT matched here, unlike the Scala
 * rule: verified via {@code javap} against the real {@code
 * spark-sql_2.11-2.4.8.jar} (2026-09-24) that {@code functions.xxhash64}
 * does not exist at all in 2.4.8 (it was added in Spark 3.0) -- a 2.4.8
 * Java codebase cannot call a method that doesn't exist yet, the same
 * "verify Java-reachability before writing" discipline that already dropped
 * {@code grouping_id}'s Java counterpart (STATUS.md SS2.5).
 */
public class SparkHashOnMapTypeDetect extends Recipe {

    private static final MethodMatcher HASH_MATCHER =
            new MethodMatcher("org.apache.spark.sql.functions hash(..)");
    private static final MethodMatcher MAP_MATCHER =
            new MethodMatcher("org.apache.spark.sql.functions map(..)");

    @Override
    public String getDisplayName() {
        return "Detect hash() applied to a map() result";
    }

    @Override
    public String getDescription() {
        return "hash() applied directly to a map(...) result, which Spark 3.0 rejects instead of hashing.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (HASH_MATCHER.matches(m) && hasMapArgument(m)) {
                    return SearchResult.found(
                            m,
                            "hash() applied to a map(...) result; Spark 3.0 throws on MapType input instead of hashing it."
                    );
                }
                return m;
            }

            private boolean hasMapArgument(J.MethodInvocation m) {
                for (Expression arg : m.getArguments()) {
                    if (arg instanceof J.MethodInvocation && MAP_MATCHER.matches((J.MethodInvocation) arg)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
