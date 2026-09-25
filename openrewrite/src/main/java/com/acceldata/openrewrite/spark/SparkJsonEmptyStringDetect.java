package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.internal.lang.Nullable;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code JsonEmptyStringDetect} rule:
 * the JSON datasource no longer accepts an empty string as a value for a
 * non-String, non-Binary field from Spark 3.0 -- 2.4 read {@code ""} as
 * null, 3.0 raises a malformed-record error. Narrowed exactly like the
 * Scala rule to a {@code .json(...)} read whose fluent-chain receiver
 * includes an explicit {@code .schema(...)} call -- with an inferred schema
 * every field that ever holds {@code ""} is inferred as a string, so there
 * is nothing to reject.
 *
 * <p>Feeds {@code spark.sql.legacy.json.allowEmptyString.enabled} -- Tier 2
 * as of 2026-09-25, cross-linked into the existing {@code LegacyConfigRegistry}
 * entry's {@code detectionRuleIds} so Phase B (spark-migrate-cli's
 * {@code PhaseBRunner}) actually injects it.
 */
public class SparkJsonEmptyStringDetect extends Recipe {

    private static final MethodMatcher JSON_MATCHER =
            new MethodMatcher("org.apache.spark.sql.DataFrameReader json(..)");
    private static final MethodMatcher SCHEMA_MATCHER =
            new MethodMatcher("org.apache.spark.sql.DataFrameReader schema(..)");

    @Override
    public String getDisplayName() {
        return "Detect a JSON read with an explicit schema";
    }

    @Override
    public String getDescription() {
        return "A JSON read with an explicit schema; from Spark 3.0 an empty string is rejected for non-string fields instead of read as null.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (JSON_MATCHER.matches(m) && hasRealArgument(m) && chainHasSchema(m.getSelect())) {
                    return SearchResult.found(
                            m,
                            "JSON read with an explicit schema. From Spark 3.0 an empty string is no longer " +
                                    "accepted as a value for a non-String/non-Binary field -- 2.4 read it as null, " +
                                    "3.0 treats the record as malformed."
                    );
                }
                return m;
            }

            private boolean hasRealArgument(J.MethodInvocation m) {
                return !m.getArguments().isEmpty() && !(m.getArguments().get(0) instanceof J.Empty);
            }

            private boolean chainHasSchema(@Nullable Expression receiver) {
                if (!(receiver instanceof J.MethodInvocation)) {
                    return false;
                }
                J.MethodInvocation call = (J.MethodInvocation) receiver;
                if (SCHEMA_MATCHER.matches(call)) {
                    return true;
                }
                return chainHasSchema(call.getSelect());
            }
        };
    }
}
