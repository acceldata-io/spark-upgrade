package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code NegativeDecimalScaleDetect}
 * rule: a negative decimal scale ({@code new DecimalType(precision, scale)}
 * with {@code scale < 0}) is rejected by Spark 3.0's analyzer by default.
 * Narrowed to a literal negative {@code int} scale argument, same as the
 * Scala rule -- the only shape that's statically certain without evaluating
 * an arbitrary expression. Feeds {@code spark.sql.legacy.allowNegativeScaleOfDecimal}
 * -- Tier 2 as of 2026-09-25, cross-linked into the existing {@code
 * LegacyConfigRegistry} entry's {@code detectionRuleIds} so Phase B
 * (spark-migrate-cli's {@code PhaseBRunner}) actually injects it. Constructor
 * verified present in the real {@code spark-catalyst_2.11-2.4.8.jar} via
 * {@code javap} (2026-09-24). Like
 * scalameta, javac's grammar (and OpenRewrite's parser, which mirrors it)
 * parses {@code -2} as unary negation of the literal {@code 2}
 * ({@code J.Unary(Negative, J.Literal(2))}), not as a single negative
 * literal token -- {@code literalValue} below handles both shapes rather
 * than assuming either one.
 */
public class SparkNegativeDecimalScaleDetect extends Recipe {

    private static final MethodMatcher CTOR_MATCHER =
            new MethodMatcher("org.apache.spark.sql.types.DecimalType <constructor>(int, int)");

    @Override
    public String getDisplayName() {
        return "Detect a negative DecimalType scale";
    }

    @Override
    public String getDescription() {
        return "DecimalType(precision, scale) constructed with a literal negative scale, which Spark 3.0 rejects by default.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.NewClass visitNewClass(J.NewClass newClass, ExecutionContext ctx) {
                J.NewClass n = super.visitNewClass(newClass, ctx);
                if (CTOR_MATCHER.matches(n) && n.getArguments().size() == 2) {
                    Object scale = literalValue(n.getArguments().get(1));
                    if (scale instanceof Integer && (Integer) scale < 0) {
                        return SearchResult.found(
                                n,
                                "DecimalType(..., " + scale + ") uses a negative scale, which Spark 3.0 rejects at " +
                                        "analysis time by default."
                        );
                    }
                }
                return n;
            }

            private Object literalValue(org.openrewrite.java.tree.Expression e) {
                if (e instanceof J.Literal) {
                    return ((J.Literal) e).getValue();
                }
                if (e instanceof J.Unary && ((J.Unary) e).getOperator() == J.Unary.Type.Negative
                        && ((J.Unary) e).getExpression() instanceof J.Literal) {
                    Object v = ((J.Literal) ((J.Unary) e).getExpression()).getValue();
                    return v instanceof Integer ? -(Integer) v : null;
                }
                return null;
            }
        };
    }
}
