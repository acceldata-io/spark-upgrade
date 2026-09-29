package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

import java.util.List;

/**
 * Java-side counterpart to the Scalafix {@code DuplicateMapKeyLiteralDetect}
 * rule (2026-09-29 coverage review, ported): {@code functions.map(k1, v1,
 * k2, v2, ...)} with two equal literal keys now throws a {@code
 * RuntimeException} in Spark 3.0 onward (governed by {@code
 * spark.sql.mapKeyDedupPolicy}), instead of silently keeping one arbitrary
 * value the way 2.4 did. The varargs {@code Column...} overload was
 * confirmed present via {@code javap} against the real {@code
 * spark-sql_2.11-2.4.8.jar} (2026-09-29).
 *
 * <p>Only flags a literal-vs-literal duplicate (two key arguments that are
 * both {@link J.Literal} with an equal value) -- a dynamic key expression
 * can't be compared this way without evaluating it, and guessing would risk
 * a false positive on a merely similar-looking key, so those are silently
 * left unflagged rather than over-reported. Detect-only (Tier 3): the
 * intended fix (drop the duplicate, or was a different key meant?) is a
 * business-logic question, not a mechanical rewrite.
 */
public class SparkDuplicateMapKeyLiteralDetect extends Recipe {

    private static final MethodMatcher MAP_MATCHER =
            new MethodMatcher("org.apache.spark.sql.functions map(org.apache.spark.sql.Column...)");
    private static final MethodMatcher LIT_MATCHER =
            new MethodMatcher("org.apache.spark.sql.functions lit(java.lang.Object)");

    @Override
    public String getDisplayName() {
        return "Detect a functions.map(...) literal with two equal literal keys";
    }

    @Override
    public String getDescription() {
        return "functions.map(k1, v1, k2, v2, ...) with two equal literal keys throws a RuntimeException from " +
                "Spark 3.0 onward instead of silently keeping one arbitrary value.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (MAP_MATCHER.matches(m)) {
                    List<Expression> args = m.getArguments();
                    for (int i = 0; i < args.size(); i += 2) {
                        String iKey = literalKey(args.get(i));
                        if (iKey == null) {
                            continue;
                        }
                        for (int j = i + 2; j < args.size(); j += 2) {
                            if (iKey.equals(literalKey(args.get(j)))) {
                                return SearchResult.found(m,
                                        "This map(...) literal has two equal keys (" + iKey + "); Spark 3.0+ " +
                                                "throws a RuntimeException for a duplicate map key instead of " +
                                                "silently keeping one value.");
                            }
                        }
                    }
                }
                return m;
            }

            /** A key argument to map(...) is a Column, so a literal key arrives
             * wrapped as {@code lit(<literal>)}, never as a bare {@link
             * J.Literal} itself (that wouldn't type-check against the
             * {@code Column...} varargs parameter). Unwrap one level of
             * {@code lit(...)} before checking for a literal underneath. */
            private String literalKey(Expression e) {
                Expression unwrapped = e;
                if (e instanceof J.MethodInvocation && LIT_MATCHER.matches((J.MethodInvocation) e)) {
                    List<Expression> litArgs = ((J.MethodInvocation) e).getArguments();
                    if (litArgs.size() == 1) {
                        unwrapped = litArgs.get(0);
                    }
                }
                if (unwrapped instanceof J.Literal) {
                    Object v = ((J.Literal) unwrapped).getValue();
                    return v == null ? null : v.toString();
                }
                return null;
            }
        };
    }
}
