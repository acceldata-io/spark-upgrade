package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

import java.time.DateTimeException;
import java.time.ZoneId;

/**
 * Java-side counterpart to the Scalafix {@code InvalidTimeZoneIdDetect}
 * rule: an invalid timezone ID silently fell back to GMT before Spark 3.0,
 * and throws from 3.0 onward. A literal timezone string is exactly decidable
 * with no type inference needed, and the validator is pure JDK ({@code
 * java.time.ZoneId}, the same resolution Spark itself performs), so this
 * recipe needs nothing from Spark's own classpath to be correct -- ported
 * directly, not re-derived, from the Scala rule's own hard-won validator
 * (getting {@code EST}/{@code PST}/{@code CST}/{@code MST}/{@code IST} to
 * resolve via {@code ZoneId.SHORT_IDS} instead of being flagged as invalid
 * was the whole difficulty there; verified unchanged here since it's the
 * same JDK call).
 *
 * <p>{@code from_utc_timestamp}/{@code to_utc_timestamp}'s {@code
 * (Column, String)} overloads verified present in the real {@code
 * spark-sql_2.11-2.4.8.jar} via {@code javap} (2026-09-24).
 */
public class SparkInvalidTimeZoneIdDetect extends Recipe {

    private static final MethodMatcher FROM_UTC_MATCHER =
            new MethodMatcher("org.apache.spark.sql.functions from_utc_timestamp(org.apache.spark.sql.Column, java.lang.String)");
    private static final MethodMatcher TO_UTC_MATCHER =
            new MethodMatcher("org.apache.spark.sql.functions to_utc_timestamp(org.apache.spark.sql.Column, java.lang.String)");
    private static final String SESSION_TIMEZONE_KEY = "spark.sql.session.timeZone";

    @Override
    public String getDisplayName() {
        return "Detect an unresolvable literal timezone ID";
    }

    @Override
    public String getDescription() {
        return "Flags a literal timezone ID that Spark 3.x rejects; 2.4 silently fell back to GMT instead of throwing.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);

                if ((FROM_UTC_MATCHER.matches(m) || TO_UTC_MATCHER.matches(m)) && m.getArguments().size() == 2) {
                    String tz = literalString(m.getArguments().get(1));
                    if (tz != null && !isResolvable(tz)) {
                        return report(m, tz, "passed to a *_utc_timestamp function");
                    }
                }

                String name = m.getSimpleName();
                if (("config".equals(name) || "set".equals(name)) && m.getArguments().size() == 2) {
                    String key = literalString(m.getArguments().get(0));
                    String tz = literalString(m.getArguments().get(1));
                    if (SESSION_TIMEZONE_KEY.equals(key) && tz != null && !isResolvable(tz)) {
                        return report(m, tz, "set as " + SESSION_TIMEZONE_KEY);
                    }
                }

                return m;
            }

            private J.MethodInvocation report(J.MethodInvocation m, String tz, String where) {
                return SearchResult.found(
                        m,
                        "Timezone ID \"" + tz + "\" (" + where + ") is not resolvable by java.time (checked with " +
                                "ZoneId.of(id, ZoneId.SHORT_IDS), the same resolution Spark performs). Spark 2.4 " +
                                "silently fell back to GMT for an unrecognised ID; Spark 3.0 and later throw instead."
                );
            }

            private String literalString(Expression e) {
                if (e instanceof J.Literal && ((J.Literal) e).getValue() instanceof String) {
                    return (String) ((J.Literal) e).getValue();
                }
                return null;
            }

            private boolean isResolvable(String id) {
                try {
                    ZoneId.of(id, ZoneId.SHORT_IDS);
                    return true;
                } catch (DateTimeException e) {
                    return false;
                }
            }
        };
    }
}
