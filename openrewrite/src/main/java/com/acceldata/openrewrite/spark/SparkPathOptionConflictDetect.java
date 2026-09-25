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
 * Java-side counterpart to the Scalafix {@code PathOptionConflictDetect}
 * rule: since Spark 3.1, a {@code path} option can no longer coexist with a
 * path argument to {@code DataFrameReader#load}/{@code DataFrameWriter#save}
 * -- purely syntactic, high confidence. Detect-only: the paired legacy config
 * ({@code spark.sql.legacy.pathOptionBehavior.enabled}) already exists in
 * {@code LegacyConfigRegistry} on the Scala side (config existence doesn't
 * depend on source language) -- Tier 2 as of 2026-09-25, this recipe's id
 * cross-linked into that same config's {@code detectionRuleIds} so Phase B
 * (spark-migrate-cli's {@code PhaseBRunner}) actually injects it.
 *
 * <p>Matches by {@link MethodMatcher} against the real {@code
 * DataFrameReader}/{@code DataFrameWriter} signatures (verified via {@code
 * javap} against the real 2.4.8 jar, 2026-09-24), not by method name alone --
 * the original implementation fired on any method literally named {@code
 * load}/{@code save} with an argument, which is a false positive on any
 * unrelated builder of that shape (code review 2026-09-24 SS4.5). This is
 * also what unblocks promoting this recipe off Tier 3 later: a name-only
 * match must never be promoted, a type-checked one safely can be once proven.
 */
public class SparkPathOptionConflictDetect extends Recipe {

    private static final MethodMatcher LOAD_MATCHER =
            new MethodMatcher("org.apache.spark.sql.DataFrameReader load(..)");
    private static final MethodMatcher SAVE_MATCHER =
            new MethodMatcher("org.apache.spark.sql.DataFrameWriter save(..)");

    @Override
    public String getDisplayName() {
        return "Detect a path option conflicting with a path argument to load()/save()";
    }

    @Override
    public String getDescription() {
        return "A `path` option and a path argument to load()/save() coexisting on the same chain; " +
                "Spark 3.1+ rejects this instead of silently choosing one.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                String name = m.getSimpleName();
                boolean isLoadOrSave = LOAD_MATCHER.matches(m) || SAVE_MATCHER.matches(m);
                if (isLoadOrSave && hasRealArgument(m) && chainHasPathOption(m.getSelect())) {
                    return SearchResult.found(
                            m,
                            "A `path` option and a path argument to ." + name + "(...) coexist on the same chain; " +
                                    "Spark 3.1+ rejects this instead of silently choosing one."
                    );
                }
                return m;
            }
        };
    }

    private static boolean hasRealArgument(J.MethodInvocation m) {
        return !m.getArguments().isEmpty() && !(m.getArguments().get(0) instanceof J.Empty);
    }

    /** Walks the fluent-chain receiver back to its root, looking for any
     * {@code .option("path", ...)} call anywhere along the chain -- not just
     * the immediate receiver, since {@code .option(...)} may appear before
     * other calls (e.g. {@code .format(...)}) in the chain. */
    private static boolean chainHasPathOption(@Nullable Expression receiver) {
        if (!(receiver instanceof J.MethodInvocation)) {
            return false;
        }
        J.MethodInvocation call = (J.MethodInvocation) receiver;
        if ("option".equals(call.getSimpleName()) && isPathKey(call)) {
            return true;
        }
        return chainHasPathOption(call.getSelect());
    }

    private static boolean isPathKey(J.MethodInvocation optionCall) {
        if (optionCall.getArguments().isEmpty()) {
            return false;
        }
        Expression firstArg = optionCall.getArguments().get(0);
        return firstArg instanceof J.Literal
                && "path".equals(((J.Literal) firstArg).getValue());
    }
}
