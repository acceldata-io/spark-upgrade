package com.acceldata.openrewrite.spark;

import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;

/**
 * {@code TaskContext.isRunningLocally()} is removed in Spark 3.0. In 2.4.8 it
 * already returned a constant: {@code TaskContextImpl.isRunningLocally} is
 * {@code iconst_0; ireturn} ({@code javap -c}), and {@code
 * BarrierTaskContext} delegates to it. So the call becomes the literal
 * {@code false}, which is exact when evaluating the receiver has no side
 * effects: {@code TaskContext.get()}, a variable, or a field.
 *
 * <p>Not rewritten, and reported by {@link SparkIsRunningLocallyDetect}
 * instead: any other receiver, and a call inside a loop condition, where a
 * constant could make the loop body or the code after it unreachable, which
 * javac rejects.
 */
public class SparkIsRunningLocallyRewrite extends Recipe {

    private static final String TASK_CONTEXT = "org.apache.spark.TaskContext";
    static final MethodMatcher IS_RUNNING_LOCALLY = new MethodMatcher(TASK_CONTEXT + " isRunningLocally()");
    private static final MethodMatcher GET = new MethodMatcher(TASK_CONTEXT + " get()");

    @Override
    public String getDisplayName() {
        return "Replace TaskContext#isRunningLocally() with false";
    }

    @Override
    public String getDescription() {
        return "TaskContext.isRunningLocally() is removed in Spark 3.0. In 2.4 it always returned false, " +
                "so the call becomes the literal false.";
    }

    /** True when the call (unvisited, at {@code cursor}) is one this recipe rewrites. */
    static boolean isRewritable(J.MethodInvocation m, Cursor cursor) {
        return IS_RUNNING_LOCALLY.matches(m) && isSideEffectFree(m.getSelect()) && !inLoopCondition(cursor);
    }

    private static boolean isSideEffectFree(Expression receiver) {
        if (receiver instanceof J.Identifier) {
            return true;
        }
        if (receiver instanceof J.FieldAccess) {
            return isSideEffectFree(((J.FieldAccess) receiver).getTarget());
        }
        if (receiver instanceof J.MethodInvocation && GET.matches((J.MethodInvocation) receiver)) {
            Expression type = ((J.MethodInvocation) receiver).getSelect();
            return type == null || type instanceof J.Identifier || type instanceof J.FieldAccess;
        }
        return false;
    }

    private static boolean inLoopCondition(Cursor cursor) {
        for (Cursor c = cursor.getParentTreeCursor(); !c.isRoot(); c = c.getParentTreeCursor()) {
            Object v = c.getValue();
            if (v instanceof J.ForLoop.Control) {
                return true;
            }
            if (v instanceof J.ControlParentheses) {
                Object owner = c.getParentTreeCursor().getValue();
                return owner instanceof J.WhileLoop || owner instanceof J.DoWhileLoop;
            }
            if (v instanceof J.Block || v instanceof J.Lambda || v instanceof J.ClassDeclaration) {
                return false;
            }
        }
        return false;
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaVisitor<ExecutionContext>() {
            @Override
            public J visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                boolean rewrite = isRewritable(method, getCursor());
                J j = super.visitMethodInvocation(method, ctx);
                if (!rewrite || !(j instanceof J.MethodInvocation)) {
                    return j;
                }
                maybeRemoveImport(TASK_CONTEXT);
                return JavaTemplate.apply("false", getCursor(), ((J.MethodInvocation) j).getCoordinates().replace());
            }
        };
    }
}
