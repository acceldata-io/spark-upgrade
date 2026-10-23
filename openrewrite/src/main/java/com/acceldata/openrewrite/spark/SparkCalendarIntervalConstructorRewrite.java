package com.acceldata.openrewrite.spark;

import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.TypeUtils;
import org.openrewrite.marker.Markers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@code new CalendarInterval(months, microseconds)} does not compile on Spark
 * 3.x, which added a separate {@code days} field: the only constructor is
 * {@code (int months, int days, long microseconds)} ({@code javap}, 3.5.5).
 * The rewrite inserts {@code 0} days: {@code new CalendarInterval(m, 0, us)}
 * holds the same value. Added to a timestamp it gives 2.4.8's instants on
 * 3.5.5, also across a DST change, where splitting whole days out of {@code
 * us} would not (probed, America/Los_Angeles). The output
 * needs 3.x.
 *
 * <p>Spark names an unaliased {@code lit(interval)} column after the value, and
 * 3.5.5 prints it differently ({@code INTERVAL '1 months 25 hours'}, 2.4.8:
 * {@code interval 1 months 1 days 1 hours}). No 3.x value gives 2.4's name,
 * so a constructor that is the argument of a Column expression is rewritten
 * only where that column's name is discarded ({@link ColumnNames}).
 *
 * <p>Only the constructor. Code that reads {@code months}/{@code
 * microseconds}, or uses the removed statics ({@code fromString}, {@code
 * MICROS_PER_*}), stays with {@link SparkCalendarIntervalUsageDetect}.
 */
public class SparkCalendarIntervalConstructorRewrite extends Recipe {

    private static final MethodMatcher CONSTRUCTOR =
            new MethodMatcher("org.apache.spark.unsafe.types.CalendarInterval <constructor>(int, long)");

    /** True when the constructor call (unvisited, at {@code cursor}) is one this recipe rewrites. */
    static boolean isRewritable(J.NewClass n, Cursor cursor) {
        if (!CONSTRUCTOR.matches(n)) {
            return false;
        }
        Cursor parentCursor = cursor.getParentTreeCursor();
        Object parent = parentCursor.getValue();
        boolean inColumn = parent instanceof J.MethodInvocation &&
                TypeUtils.isOfClassType(((J.MethodInvocation) parent).getType(), "org.apache.spark.sql.Column");
        return !inColumn || ColumnNames.isDiscarded((J) parent, parentCursor);
    }

    @Override
    public String getDisplayName() {
        return "Add the days argument to new CalendarInterval(months, microseconds)";
    }

    @Override
    public String getDescription() {
        return "Spark 3.x's CalendarInterval has a days field and only a (months, days, microseconds) constructor. " +
                "new CalendarInterval(m, us) becomes new CalendarInterval(m, 0, us), the same value.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.NewClass visitNewClass(J.NewClass newClass, ExecutionContext ctx) {
                boolean rewrite = isRewritable(newClass, getCursor());
                J.NewClass n = super.visitNewClass(newClass, ctx);
                if (!rewrite) {
                    return n;
                }
                List<Expression> args = new ArrayList<>(n.getArguments());
                Expression microseconds = args.get(1);
                args.add(1, new J.Literal(Tree.randomId(), microseconds.getPrefix(), Markers.EMPTY, 0, "0", null,
                        JavaType.Primitive.Int));
                J.NewClass rewritten = n.withArguments(args);
                JavaType.Method type = n.getConstructorType();
                if (type != null) {
                    rewritten = rewritten.withConstructorType(type
                            .withParameterNames(Arrays.asList("months", "days", "microseconds"))
                            .withParameterTypes(Arrays.asList(JavaType.Primitive.Int, JavaType.Primitive.Int, JavaType.Primitive.Long)));
                }
                return rewritten;
            }
        };
    }
}
