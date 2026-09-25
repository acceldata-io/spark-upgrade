package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code IsRunningLocallyWarn} rule:
 * {@code TaskContext.isRunningLocally()} was removed in Spark 3.0 along with
 * local execution, so any code path guarded by it is dead. Verified present
 * (as an abstract method on the {@code TaskContext} class, plain Java-visible)
 * in the real {@code spark-core_2.11-2.4.8.jar} via {@code javap}
 * (2026-09-24).
 */
public class SparkIsRunningLocallyDetect extends Recipe {

    private static final MethodMatcher MATCHER =
            new MethodMatcher("org.apache.spark.TaskContext isRunningLocally()");

    @Override
    public String getDisplayName() {
        return "Detect TaskContext#isRunningLocally";
    }

    @Override
    public String getDescription() {
        return "TaskContext.isRunningLocally() was removed in Spark 3.0 along with local execution, so the code path it guards is dead.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (MATCHER.matches(m)) {
                    return SearchResult.found(
                            m,
                            "TaskContext.isRunningLocally() was removed in Spark 3.0 along with local execution; the code path it guards is dead."
                    );
                }
                return m;
            }
        };
    }
}
