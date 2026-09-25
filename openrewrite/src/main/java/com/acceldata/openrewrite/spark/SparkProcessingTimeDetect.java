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
 * Java-side counterpart to the Scalafix {@code MigrateTrigger} rule:
 * {@code org.apache.spark.sql.streaming.ProcessingTime} is removed in favor
 * of {@code Trigger.ProcessingTime(...)}/{@code Trigger.Once()}/{@code
 * Trigger.Continuous(...)}. Scala emits static forwarder methods for a
 * case-class companion object with no naming clash, so {@code
 * ProcessingTime.apply(long)}/{@code ProcessingTime.create(long, TimeUnit)}
 * are genuinely callable from Java exactly like any other static method --
 * confirmed via {@code javap} against the real {@code
 * spark-sql_2.11-2.4.8.jar} (2026-09-24), where both show up as ordinary
 * {@code public static} methods on the {@code ProcessingTime} class itself,
 * not only on its Scala-only {@code MODULE$} singleton.
 *
 * <p>Detect-only rather than an auto-rewrite: the exact-rename precedent this
 * session promoted to Tier 1 ({@code SparkUnionAllRename}, {@code
 * SparkShuffleWriteMetricsRename}) only ever needs {@code ChangeMethodName}
 * to change the METHOD on the same class. This rewrite needs to change both
 * the target class ({@code ProcessingTime} &#8594; {@code Trigger}) and the
 * method name ({@code apply}/{@code create} &#8594; {@code ProcessingTime})
 * at once, which needs a {@code JavaTemplate}-based rewrite with its own
 * classpath-resolution setup -- not attempted this pass rather than risk a
 * composed-recipe rewrite nobody verified end to end.
 */
public class SparkProcessingTimeDetect extends Recipe {

    private static final List<MethodMatcher> MATCHERS = Arrays.asList(
            new MethodMatcher("org.apache.spark.sql.streaming.ProcessingTime apply(long)"),
            new MethodMatcher("org.apache.spark.sql.streaming.ProcessingTime create(long, java.util.concurrent.TimeUnit)")
    );

    @Override
    public String getDisplayName() {
        return "Detect the removed ProcessingTime factory";
    }

    @Override
    public String getDescription() {
        return "org.apache.spark.sql.streaming.ProcessingTime is removed in Spark 3.x; use Trigger.ProcessingTime(...) instead.";
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
                                "ProcessingTime is removed in Spark 3.x; use Trigger.ProcessingTime(...) instead " +
                                        "(same argument shape)."
                        );
                    }
                }
                return m;
            }
        };
    }
}
