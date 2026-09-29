package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side rule closing a 2026-09-29 coverage-review gap against the
 * Spark core migration guide: {@code AccumulableInfo.apply(...)} is removed
 * in Spark 3.0 (it "is not supposed to be called by end users"), and direct
 * construction goes with it. Both the companion object's {@code apply}
 * overloads (3/4/5/7-arg Java-friendly convenience factories) and the raw
 * 7-arg constructor were confirmed present and callable from Java via
 * {@code javap} against the real {@code spark-core_2.11-2.4.8.jar}
 * (2026-09-29) -- {@code org.apache.spark.scheduler.AccumulableInfo} is an
 * ordinary Scala case class, so its generated {@code <init>} and companion
 * {@code apply} are both plain {@code public static}/{@code public}
 * members, not Scala-only.
 *
 * <p>Detect-only (Tier 3, per this session's policy for every new rule):
 * there is no single safe replacement expression -- {@code AccumulableInfo}
 * itself is only ever constructed internally by Spark's own accumulator
 * plumbing, so a customer's own call to it needs individual review of why
 * it's there at all, not a mechanical rewrite.
 */
public class SparkAccumulableInfoConstructionDetect extends Recipe {

    private static final MethodMatcher APPLY_MATCHER =
            new MethodMatcher("org.apache.spark.scheduler.AccumulableInfo apply(..)");
    private static final MethodMatcher CTOR_MATCHER =
            new MethodMatcher("org.apache.spark.scheduler.AccumulableInfo <constructor>(..)");

    @Override
    public String getDisplayName() {
        return "Detect direct AccumulableInfo construction";
    }

    @Override
    public String getDescription() {
        return "AccumulableInfo.apply(...) and direct AccumulableInfo construction are removed in Spark 3.0; " +
                "this type is internal plumbing Spark's own accumulator machinery constructs, not a public API.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (APPLY_MATCHER.matches(m)) {
                    return SearchResult.found(m,
                            "AccumulableInfo.apply(...) is removed in Spark 3.0 -- this type is internal " +
                                    "plumbing, not meant to be constructed directly.");
                }
                return m;
            }

            @Override
            public J.NewClass visitNewClass(J.NewClass newClass, ExecutionContext ctx) {
                J.NewClass n = super.visitNewClass(newClass, ctx);
                if (CTOR_MATCHER.matches(n)) {
                    return SearchResult.found(n,
                            "Direct AccumulableInfo construction is removed in Spark 3.0 -- this type is " +
                                    "internal plumbing, not meant to be constructed directly.");
                }
                return n;
            }
        };
    }
}
