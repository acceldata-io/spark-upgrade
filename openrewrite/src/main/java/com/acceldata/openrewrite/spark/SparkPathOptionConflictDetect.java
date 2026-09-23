package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.internal.lang.Nullable;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code PathOptionConflictDetect}
 * rule: since Spark 3.1, a {@code path} option can no longer coexist with a
 * path argument to {@code DataFrameReader#load}/{@code DataFrameWriter#save}
 * -- purely syntactic, high confidence. Detect-only: the paired legacy config
 * ({@code spark.sql.legacy.pathOptionBehavior.enabled}) already exists in
 * {@code LegacyConfigRegistry} on the Scala side and is reusable as-is
 * (config existence doesn't depend on source language), but Phase B/
 * config-injection wiring is out of scope for this analyze-only pass.
 */
public class SparkPathOptionConflictDetect extends Recipe {

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
                boolean isLoadOrSave = "load".equals(name) || "save".equals(name);
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
