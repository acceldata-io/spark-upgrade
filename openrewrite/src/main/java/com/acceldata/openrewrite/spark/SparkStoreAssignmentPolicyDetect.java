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
 * Java-side counterpart to the Scalafix {@code StoreAssignmentPolicyDetect}
 * rule: from Spark 3.0 a write into an existing table's schema follows
 * {@code spark.sql.storeAssignmentPolicy=ANSI}, so a value that cannot be
 * safely cast to its column's type (string to int or date) fails the write at
 * analysis, where 2.4 cast it. Flags {@code insertInto} and
 * {@code saveAsTable} in append mode -- {@code SaveMode.Append}, the string
 * {@code "append"}, or a mode it cannot read. Any other mode takes the table's
 * schema from the DataFrame and never fails. Embedded SQL {@code INSERT}s are
 * the shared SQL linter's, in spark-migrate-cli.
 *
 * <p>Feeds {@code spark.sql.storeAssignmentPolicy} (Tier 2).
 */
public class SparkStoreAssignmentPolicyDetect extends Recipe {

    private static final MethodMatcher INSERT_INTO =
            new MethodMatcher("org.apache.spark.sql.DataFrameWriter insertInto(..)");
    private static final MethodMatcher SAVE_AS_TABLE =
            new MethodMatcher("org.apache.spark.sql.DataFrameWriter saveAsTable(..)");
    private static final MethodMatcher MODE =
            new MethodMatcher("org.apache.spark.sql.DataFrameWriter mode(..)");

    private static final String WHY =
            " writes into an existing table's schema. From Spark 3.0 (spark.sql.storeAssignmentPolicy=ANSI) a value " +
                    "that cannot be safely cast to its column's type -- string to int or date, boolean to numeric -- " +
                    "fails the write at analysis, and a numeric overflow fails at run time; 2.4 cast it.";

    @Override
    public String getDisplayName() {
        return "Detect a write into an existing table's schema";
    }

    @Override
    public String getDescription() {
        return "insertInto, or saveAsTable in append mode; from Spark 3.0 an unsafe type conversion fails the write instead of casting.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (INSERT_INTO.matches(m)) {
                    return SearchResult.found(m, "insertInto" + WHY);
                }
                if (SAVE_AS_TABLE.matches(m)) {
                    Boolean append = appendMode(m.getSelect());
                    if (append == null || append) {
                        return SearchResult.found(m, (append == null ? "saveAsTable with a mode that may be append" : "saveAsTable in append mode") + WHY);
                    }
                }
                return m;
            }

            /** TRUE for an append mode on the writer chain, FALSE for any
             * other mode or none, null for a mode it cannot read. */
            private @Nullable Boolean appendMode(@Nullable Expression receiver) {
                if (!(receiver instanceof J.MethodInvocation)) {
                    return false;
                }
                J.MethodInvocation call = (J.MethodInvocation) receiver;
                if (MODE.matches(call) && call.getArguments().size() == 1) {
                    Expression arg = call.getArguments().get(0);
                    if (arg instanceof J.Literal && ((J.Literal) arg).getValue() instanceof String) {
                        return "append".equalsIgnoreCase((String) ((J.Literal) arg).getValue());
                    }
                    if (arg instanceof J.FieldAccess) {
                        return "Append".equals(((J.FieldAccess) arg).getSimpleName());
                    }
                    if (arg instanceof J.Identifier) {
                        String name = ((J.Identifier) arg).getSimpleName();
                        return name.equals("Append") ? Boolean.TRUE : null;
                    }
                    return null;
                }
                return appendMode(call.getSelect());
            }
        };
    }
}
