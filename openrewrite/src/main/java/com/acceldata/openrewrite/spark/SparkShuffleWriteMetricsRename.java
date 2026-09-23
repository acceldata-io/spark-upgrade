package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Java-side counterpart to the Scalafix {@code ShuffleWriteMetricsRenameDetect}
 * rule: three deprecated {@code ShuffleWriteMetrics} accessors were removed in
 * Spark 3.0, each in favor of an exactly-equivalent shorter name. All six
 * method names (three old, three new) verified present in the real {@code
 * spark-core_2.11-2.4.8.jar} via {@code javap} (2026-09-21) -- same bytecode a
 * Java caller hits identically, so this rewrite is valid pre-migration too.
 */
public class SparkShuffleWriteMetricsRename extends Recipe {

    private static final Map<MethodMatcher, String> RENAMES = new LinkedHashMap<>();

    static {
        RENAMES.put(new MethodMatcher("org.apache.spark.executor.ShuffleWriteMetrics shuffleBytesWritten()"), "bytesWritten");
        RENAMES.put(new MethodMatcher("org.apache.spark.executor.ShuffleWriteMetrics shuffleWriteTime()"), "writeTime");
        RENAMES.put(new MethodMatcher("org.apache.spark.executor.ShuffleWriteMetrics shuffleRecordsWritten()"), "recordsWritten");
    }

    @Override
    public String getDisplayName() {
        return "Rename removed ShuffleWriteMetrics accessors";
    }

    @Override
    public String getDescription() {
        return "shuffleBytesWritten/shuffleWriteTime/shuffleRecordsWritten were removed in Spark 3.0; " +
                "renamed to bytesWritten/writeTime/recordsWritten.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                for (Map.Entry<MethodMatcher, String> entry : RENAMES.entrySet()) {
                    if (entry.getKey().matches(m)) {
                        return m.withName(m.getName().withSimpleName(entry.getValue()));
                    }
                }
                return m;
            }
        };
    }
}
