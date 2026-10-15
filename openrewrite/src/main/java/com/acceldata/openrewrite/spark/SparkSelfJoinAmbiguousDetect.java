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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Java-side counterpart to the Scalafix {@code SelfJoinAmbiguousColumnDetect}
 * rule: since Spark 3.0, {@code spark.sql.analyzer.failAmbiguousSelfJoin}
 * fails an ambiguous self-join at analysis time instead of silently
 * resolving column references. Matches {@code df.join(df, ...)} where the
 * join receiver and the first argument are the same simple name (two
 * differently-named variables are never flagged; an aliased reference to the
 * same Dataset is not caught).
 *
 * <p>Narrowed (2026-10-15) like the Scala rule, by a probe on 2.4.8 and
 * 3.5.5: what fails is a column referenced through the frame --
 * {@code df.col("n").gt(df.col("n"))} in the condition, or
 * {@code .select(df.col("v"))} after the join. A join on a column name
 * ({@code df.join(df, "id")}) or on same-name equalities
 * ({@code df.col("id").equalTo(df.col("id"))}, joined by {@code and}) gives
 * 2.4's rows, and {@code .drop(df.col("v"))} after it 2.4's columns. Such a
 * join is not flagged unless a later {@code df.col(..)}/{@code df.apply(..)}
 * in the same method, outside {@code drop(...)}, references the frame.
 *
 * <p>Feeds {@code spark.sql.analyzer.failAmbiguousSelfJoin}.
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
        return "df.join(df, ...) self-joins using the exact same DataFrame reference on both sides, where a column is " +
                "referenced through that frame -- which Spark 3.0 fails at analysis time as ambiguous.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (!JOIN_MATCHER.matches(m) || m.getArguments().isEmpty() || !sameIdentifier(m.getSelect(), m.getArguments().get(0))) {
                    return m;
                }
                String frame = ((J.Identifier) m.getSelect()).getSimpleName();
                boolean safe = m.getArguments().size() > 1 && safeCondition(m.getArguments().get(1), frame)
                        && !laterReference(method, frame);
                if (safe) {
                    return m;
                }
                return SearchResult.found(
                        m,
                        "df.join(df, ...) joins a DataFrame to itself using the exact same reference on both " +
                                "sides, and a column is referenced through that frame (in the condition or after the " +
                                "join); Spark 3.0's analyzer fails this as an ambiguous self-join where 2.4 silently " +
                                "resolved it to one side."
                );
            }

            private boolean sameIdentifier(Expression a, Expression b) {
                return a instanceof J.Identifier && b instanceof J.Identifier
                        && ((J.Identifier) a).getSimpleName().equals(((J.Identifier) b).getSimpleName());
            }

            /** A column name, or same-name equalities joined by and(...). */
            private boolean safeCondition(Expression c, String frame) {
                if (c instanceof J.Literal && ((J.Literal) c).getValue() instanceof String) {
                    return true;
                }
                if (c instanceof J.MethodInvocation && ((J.MethodInvocation) c).getArguments().size() == 1) {
                    J.MethodInvocation call = (J.MethodInvocation) c;
                    Expression arg = call.getArguments().get(0);
                    if (call.getSimpleName().equals("and")) {
                        return call.getSelect() != null && safeCondition(call.getSelect(), frame) && safeCondition(arg, frame);
                    }
                    if (call.getSimpleName().equals("equalTo")) {
                        String left = columnRef(call.getSelect(), frame);
                        return left != null && left.equals(columnRef(arg, frame));
                    }
                }
                return false;
            }

            /** A column referenced through {@code frame} after {@code join}
             * in its method, outside drop(...). The visit is post-order, so
             * the join is marked before any call chained on it. */
            private boolean laterReference(J.MethodInvocation join, String frame) {
                J.MethodDeclaration scope = getCursor().firstEnclosing(J.MethodDeclaration.class);
                if (scope == null) {
                    return false;
                }
                AtomicBoolean seen = new AtomicBoolean();
                AtomicBoolean found = new AtomicBoolean();
                new JavaIsoVisitor<Integer>() {
                    @Override
                    public J.MethodInvocation visitMethodInvocation(J.MethodInvocation mi, Integer p) {
                        if (mi.getSimpleName().equals("drop")) {
                            if (mi.getSelect() != null) {
                                visit(mi.getSelect(), p);
                            }
                            return mi;
                        }
                        J.MethodInvocation out = super.visitMethodInvocation(mi, p);
                        if (mi.getId().equals(join.getId())) {
                            seen.set(true);
                        } else if (seen.get() && columnRef(mi, frame) != null) {
                            found.set(true);
                        }
                        return out;
                    }
                }.visit(scope, 0);
                return found.get();
            }

            /** {@code df.col("c")} or {@code df.apply("c")} on {@code frame}: the column name. */
            private @Nullable String columnRef(@Nullable Expression e, String frame) {
                if (!(e instanceof J.MethodInvocation)) {
                    return null;
                }
                J.MethodInvocation call = (J.MethodInvocation) e;
                boolean onFrame = call.getSelect() instanceof J.Identifier
                        && ((J.Identifier) call.getSelect()).getSimpleName().equals(frame);
                if (!onFrame || !(call.getSimpleName().equals("col") || call.getSimpleName().equals("apply"))
                        || call.getArguments().size() != 1) {
                    return null;
                }
                Expression arg = call.getArguments().get(0);
                return arg instanceof J.Literal && ((J.Literal) arg).getValue() instanceof String
                        ? (String) ((J.Literal) arg).getValue() : null;
            }
        };
    }
}
