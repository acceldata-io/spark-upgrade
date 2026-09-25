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
 * Java-side counterpart to the Scalafix {@code SelfJoinAmbiguousColumnDetect}
 * rule: since Spark 3.0, {@code spark.sql.analyzer.failAmbiguousSelfJoin}
 * fails an ambiguous self-join at analysis time instead of silently
 * resolving column references. Narrowed to the same unambiguous,
 * zero-false-positive case the Scala rule uses: {@code df.join(df, ...)}
 * where the join receiver and the first argument are the exact same local
 * variable/field. Feeds {@code spark.sql.analyzer.failAmbiguousSelfJoin} --
 * Tier 2 as of 2026-09-25, cross-linked into the existing {@code
 * LegacyConfigRegistry} entry's {@code detectionRuleIds} so Phase B
 * (spark-migrate-cli's {@code PhaseBRunner}) actually injects it.
 *
 * <p>Unlike the Scala rule, this matches by identifier name rather than a
 * resolved symbol: OpenRewrite's Java LST doesn't carry the same kind of
 * cheap symbol-equality check {@code scalafix.Symbol} does for a local
 * variable, so this is narrower on purpose -- it only fires when both sides
 * are the literal same simple name, which cannot false-positive (two
 * differently-named variables are never flagged) but can under-report (an
 * aliased or field-qualified reference to the same Dataset isn't caught).
 * That's the same "smaller exact signal over a broad uncertain one"
 * trade-off {@code CalendarIntervalUsageDetect} already documents on the
 * Scala side for a different rule.
 */
public class SparkSelfJoinAmbiguousDetect extends Recipe {

    private static final MethodMatcher JOIN_MATCHER =
            new MethodMatcher("org.apache.spark.sql.Dataset join(..)");

    @Override
    public String getDisplayName() {
        return "Detect a self-join on the exact same Dataset reference";
    }

    @Override
    public String getDescription() {
        return "df.join(df, ...) self-joins using the exact same DataFrame reference on both sides, which Spark " +
                "3.0 can fail at analysis time as ambiguous.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (JOIN_MATCHER.matches(m) && !m.getArguments().isEmpty() && sameIdentifier(m.getSelect(), m.getArguments().get(0))) {
                    return SearchResult.found(
                            m,
                            "df.join(df, ...) joins a DataFrame to itself using the exact same reference on both " +
                                    "sides; Spark 3.0's analyzer can fail this as an ambiguous self-join instead of " +
                                    "resolving column references as 2.4 did."
                    );
                }
                return m;
            }

            private boolean sameIdentifier(Expression a, Expression b) {
                return a instanceof J.Identifier && b instanceof J.Identifier
                        && ((J.Identifier) a).getSimpleName().equals(((J.Identifier) b).getSimpleName());
            }
        };
    }
}
