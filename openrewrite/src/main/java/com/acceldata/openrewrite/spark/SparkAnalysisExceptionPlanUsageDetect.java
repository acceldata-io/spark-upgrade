package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side rule closing a 2026-09-29 coverage-review gap against the SQL
 * migration guide: {@code AnalysisException.plan} moves to a new
 * {@code EnhancedAnalysisException} type in Spark 3.5 -- code that calls
 * {@code .plan()} directly on a caught {@code AnalysisException} (a common
 * pattern for logging the failed query plan) stops compiling. Confirmed via
 * {@code javap} against the real {@code spark-sql_2.11-2.4.8.jar}
 * (2026-09-29): {@code plan()} is a plain public method returning
 * {@code scala.Option<LogicalPlan>}, directly callable from Java.
 *
 * <p>Detect-only (Tier 3): the fix depends on whether the caller can adopt
 * {@code EnhancedAnalysisException} (an instanceof check plus cast) or
 * needs a version-straddling helper, which is a call the customer needs to
 * make, not a mechanical rewrite.
 */
public class SparkAnalysisExceptionPlanUsageDetect extends Recipe {

    private static final MethodMatcher PLAN_MATCHER =
            new MethodMatcher("org.apache.spark.sql.AnalysisException plan()");

    @Override
    public String getDisplayName() {
        return "Detect AnalysisException#plan() usage";
    }

    @Override
    public String getDescription() {
        return "AnalysisException.plan moves to a new EnhancedAnalysisException type in Spark 3.5; a direct " +
                ".plan() call on AnalysisException stops compiling.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (PLAN_MATCHER.matches(m)) {
                    return SearchResult.found(m,
                            "AnalysisException.plan moves to EnhancedAnalysisException in Spark 3.5; this call " +
                                    "stops compiling unless the exception is narrowed to that type first.");
                }
                return m;
            }
        };
    }
}
