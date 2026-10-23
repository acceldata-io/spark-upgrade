package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;

/**
 * Since Spark 3.1, {@code load(path)}/{@code save(path)} throws an
 * AnalysisException when the same reader or writer chain also sets {@code
 * .option("path", p)}. Spark 2.4 let the argument win: 2.4.8's {@code
 * load(String)} and {@code save(String)} overwrite the {@code path} option
 * with it (probed: {@code option("path", a).load(b)}
 * reads {@code b}; {@code option("path", c).save(d)} writes {@code d} only).
 * So dropping the {@code .option("path", p)} call gives 2.4.8's result.
 *
 * <p>Only the single-path {@code DataFrameReader.load(String)} and {@code
 * DataFrameWriter.save(String)}, with the option on the same chain and a
 * value with no side effects (a literal, variable or field). A multi-path
 * {@code load(a, b)} is not: 2.4 read the option's path as well (probed).
 * {@link SparkPathOptionConflictDetect} reports every site this leaves alone.
 */
public class SparkPathOptionConflictRewrite extends Recipe {

    private static final MethodMatcher LOAD = new MethodMatcher("org.apache.spark.sql.DataFrameReader load(java.lang.String)");
    private static final MethodMatcher SAVE = new MethodMatcher("org.apache.spark.sql.DataFrameWriter save(java.lang.String)");
    private static final MethodMatcher READER_OPTION = new MethodMatcher("org.apache.spark.sql.DataFrameReader option(java.lang.String, ..)");
    private static final MethodMatcher WRITER_OPTION = new MethodMatcher("org.apache.spark.sql.DataFrameWriter option(java.lang.String, ..)");

    @Override
    public String getDisplayName() {
        return "Drop a path option that load(path)/save(path) overrides";
    }

    @Override
    public String getDescription() {
        return "From Spark 3.1, load(path)/save(path) fails when the chain also sets .option(\"path\", ...). " +
                "Spark 2.4 used the argument, so the option call is removed.";
    }

    /** A single-path load/save whose chain sets the path option, every time with a side-effect-free value. */
    static boolean isRewritable(J.MethodInvocation m) {
        if (!LOAD.matches(m) && !SAVE.matches(m)) {
            return false;
        }
        boolean found = false;
        for (Expression e = m.getSelect(); e instanceof J.MethodInvocation; e = ((J.MethodInvocation) e).getSelect()) {
            J.MethodInvocation call = (J.MethodInvocation) e;
            if (isPathOption(call)) {
                if (!isSideEffectFree(call.getArguments().get(1))) {
                    return false;
                }
                found = true;
            }
        }
        return found;
    }

    private static boolean isPathOption(J.MethodInvocation call) {
        Expression key = call.getArguments().get(0);
        return (READER_OPTION.matches(call) || WRITER_OPTION.matches(call)) &&
                key instanceof J.Literal && "path".equals(((J.Literal) key).getValue());
    }

    private static boolean isSideEffectFree(Expression e) {
        return e instanceof J.Literal || e instanceof J.Identifier ||
                (e instanceof J.FieldAccess && isSideEffectFree(((J.FieldAccess) e).getTarget()));
    }

    /** The chain with every path option call removed; each remaining call keeps its own layout. */
    private static Expression withoutPathOptions(Expression e) {
        if (!(e instanceof J.MethodInvocation)) {
            return e;
        }
        J.MethodInvocation call = (J.MethodInvocation) e;
        return isPathOption(call)
                ? withoutPathOptions(call.getSelect()).withPrefix(call.getPrefix())
                : call.withSelect(withoutPathOptions(call.getSelect()));
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                return isRewritable(m) ? m.withSelect(withoutPathOptions(m.getSelect())) : m;
            }
        };
    }
}
