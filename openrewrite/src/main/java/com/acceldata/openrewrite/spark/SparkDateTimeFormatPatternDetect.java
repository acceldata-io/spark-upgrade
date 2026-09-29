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
 * Java-side counterpart to the Scalafix {@code DateTimeFormatPatternValidator}
 * rule (2026-09-29 coverage review, ported): from Spark 3.0, {@code
 * to_date}/{@code to_timestamp}/{@code date_format}/{@code unix_timestamp}/
 * {@code from_unixtime}'s pattern argument is parsed by {@code
 * java.time.format.DateTimeFormatter} (strict, proleptic-Gregorian) instead
 * of {@code java.text.SimpleDateFormat} (lenient, hybrid Julian+Gregorian)
 * -- a pattern string that parsed leniently under 2.4 can throw at runtime
 * under 3.0's {@code EXCEPTION} policy, or silently return a different
 * result under {@code LEGACY}/{@code CORRECTED}. All five {@code (Column,
 * String)} overloads were confirmed present via {@code javap} against the
 * real {@code spark-sql_2.11-2.4.8.jar} (2026-09-29).
 *
 * <p>Fuzzy, by necessity (Tier 3, detect-only): flags every call whose
 * pattern argument is a string literal, regardless of whether that specific
 * pattern actually behaves differently under the new parser -- the same
 * "flag every call, let a human judge" posture {@code
 * SparkGroupByKeyCountWarn} takes, since correctly telling a compatible
 * pattern from an incompatible one would mean re-implementing both
 * parsers' letter tables here rather than pointing at the one guide that
 * already documents them.
 */
public class SparkDateTimeFormatPatternDetect extends Recipe {

    private static final List<MethodMatcher> MATCHERS = Arrays.asList(
            new MethodMatcher("org.apache.spark.sql.functions to_date(org.apache.spark.sql.Column, java.lang.String)"),
            new MethodMatcher("org.apache.spark.sql.functions to_timestamp(org.apache.spark.sql.Column, java.lang.String)"),
            new MethodMatcher("org.apache.spark.sql.functions date_format(org.apache.spark.sql.Column, java.lang.String)"),
            new MethodMatcher("org.apache.spark.sql.functions unix_timestamp(org.apache.spark.sql.Column, java.lang.String)"),
            new MethodMatcher("org.apache.spark.sql.functions from_unixtime(org.apache.spark.sql.Column, java.lang.String)")
    );

    @Override
    public String getDisplayName() {
        return "Detect a literal datetime format pattern";
    }

    @Override
    public String getDescription() {
        return "Datetime pattern strings are parsed by java.time.DateTimeFormatter (strict) instead of " +
                "java.text.SimpleDateFormat (lenient) from Spark 3.0 onward; review this pattern for " +
                "compatibility (see spark.sql.legacy.timeParserPolicy).";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                for (MethodMatcher matcher : MATCHERS) {
                    if (matcher.matches(m) && isStringLiteral(lastArg(m))) {
                        return SearchResult.found(m,
                                "This datetime pattern is parsed strictly from Spark 3.0 onward; review it for " +
                                        "compatibility with the old, more lenient parser.");
                    }
                }
                return m;
            }

            private org.openrewrite.java.tree.Expression lastArg(J.MethodInvocation m) {
                List<org.openrewrite.java.tree.Expression> args = m.getArguments();
                return args.isEmpty() ? null : args.get(args.size() - 1);
            }

            private boolean isStringLiteral(org.openrewrite.java.tree.Expression e) {
                return e instanceof J.Literal && ((J.Literal) e).getValue() instanceof String;
            }
        };
    }
}
