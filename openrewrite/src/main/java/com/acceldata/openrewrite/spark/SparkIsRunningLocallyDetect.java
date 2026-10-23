package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code IsRunningLocallyWarn} rule:
 * {@code TaskContext.isRunningLocally()} was removed in Spark 3.0 along with
 * local execution (2.4.8 already returned a constant false). Verified present
 * (as an abstract method on the {@code TaskContext} class, plain Java-visible)
 * in the real {@code spark-core_2.11-2.4.8.jar} via {@code javap}.
 *
 * <p>Reports only the calls {@link SparkIsRunningLocallyRewrite} leaves alone:
 * a receiver that may have side effects, or a call in a loop condition.
 */
public class SparkIsRunningLocallyDetect extends Recipe {

    @Override
    public String getDisplayName() {
        return "Detect TaskContext#isRunningLocally";
    }

    @Override
    public String getDescription() {
        return "TaskContext.isRunningLocally() was removed in Spark 3.0; in 2.4 it always returned false. Reports the calls SparkIsRunningLocallyRewrite cannot replace.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                boolean report = SparkIsRunningLocallyRewrite.IS_RUNNING_LOCALLY.matches(method) &&
                        !SparkIsRunningLocallyRewrite.isRewritable(method, getCursor());
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (report) {
                    return SearchResult.found(
                            m,
                            "TaskContext.isRunningLocally() was removed in Spark 3.0; in 2.4 it always returned false. " +
                                    "Replace the call with false (the tool did not, because of its receiver or position)."
                    );
                }
                return m;
            }
        };
    }
}
