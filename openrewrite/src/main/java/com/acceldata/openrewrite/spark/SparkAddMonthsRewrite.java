package com.acceldata.openrewrite.spark;

import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.TypeUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Restores Spark 2.4's month-end snap for {@code functions.add_months(d, n)}.
 * In 2.4, when {@code d} is the last day of its month the result is the last
 * day of the target month ({@code 2019-02-28 + 1 = 2019-03-31}); from 3.0 it
 * keeps the day number ({@code 2019-03-28}). The rewrite
 * <pre>
 *   when(to_date(d).equalTo(last_day(d)), last_day(add_months(d, n))).otherwise(add_months(d, n))
 * </pre>
 * gives 2.4.8's values on 3.5.5 for dates, timestamps, date strings, Feb 29
 * and null (probed). Every function in it exists in both
 * 2.4.8 and 3.5.5. Only {@code add_months(Column, int)} needs matching: it is
 * the one overload 2.4.8 has, so it is the only one 2.4 source can call.
 *
 * <p>Applied only where that is exact:
 * <ul>
 *   <li>{@code d} and {@code n} appear twice, so they must be safe to repeat:
 *       {@code d} is a variable, {@code col("x")}/{@code column("x")},
 *       {@code df.col("x")}/{@code df.apply("x")}, or {@code to_date}/{@code
 *       to_timestamp} of one of those; {@code n} is an int literal or a variable.</li>
 *   <li>The result's auto-generated column name must not survive. Unaliased,
 *       the column would be named {@code CASE WHEN ...} instead of {@code
 *       add_months(d, 1)}. So the call (or the Column expression it is part
 *       of) must be renamed by {@code .as}/{@code .alias}/{@code .name}, be the
 *       value of {@code withColumn}, or be a {@code filter}/{@code where}
 *       condition ({@link ColumnNames}).</li>
 * </ul>
 * Every other call is left to {@link SparkAddMonthsUsageWarn}, so each call is
 * reported by exactly one of the two. Neither touches {@code
 * last_day(add_months(...))}: the target month is the same in 2.4 and 3.x, so
 * its last day is too.
 *
 * <p>Output style follows the call: {@code functions.add_months(...)} gives
 * {@code functions.when(...)}, a static-imported {@code add_months(...)} gives
 * static imports of {@code when}/{@code to_date}/{@code last_day} (OpenRewrite's
 * import layout may fold them into {@code functions.*}), unless the file
 * declares a method with one of those names.
 */
public class SparkAddMonthsRewrite extends Recipe {

    private static final String FUNCTIONS = "org.apache.spark.sql.functions";
    private static final String COLUMN = "org.apache.spark.sql.Column";
    private static final String DATASET = "org.apache.spark.sql.Dataset";

    static final MethodMatcher ADD_MONTHS = new MethodMatcher(FUNCTIONS + " add_months(" + COLUMN + ", int)");

    private static final List<MethodMatcher> COLUMN_BY_NAME = Arrays.asList(
            new MethodMatcher(FUNCTIONS + " col(java.lang.String)"),
            new MethodMatcher(FUNCTIONS + " column(java.lang.String)"));
    private static final List<MethodMatcher> DATASET_COLUMN_BY_NAME = Arrays.asList(
            new MethodMatcher(DATASET + " col(java.lang.String)"),
            new MethodMatcher(DATASET + " apply(java.lang.String)"));
    private static final List<MethodMatcher> DATE_CONVERSIONS = Arrays.asList(
            new MethodMatcher(FUNCTIONS + " to_date(..)"),
            new MethodMatcher(FUNCTIONS + " to_timestamp(..)"));
    private static final MethodMatcher LAST_DAY = new MethodMatcher(FUNCTIONS + " last_day(" + COLUMN + ")");
    private static final MethodMatcher WHEN = new MethodMatcher(FUNCTIONS + " when(" + COLUMN + ", java.lang.Object)");
    private static final MethodMatcher EQUAL_TO = new MethodMatcher(COLUMN + " equalTo(java.lang.Object)");
    private static final MethodMatcher OTHERWISE = new MethodMatcher(COLUMN + " otherwise(java.lang.Object)");

    private static final List<String> ADDED_FUNCTIONS = Arrays.asList("when", "to_date", "last_day");

    // Real type attribution for the template output without Spark on the
    // runner's own classpath.
    private static final String[] STUBS = {
            "package org.apache.spark.sql;\n" +
            "public class Column {\n" +
            "    public Column equalTo(Object other) { return null; }\n" +
            "    public Column otherwise(Object value) { return null; }\n" +
            "}\n",
            "package org.apache.spark.sql;\n" +
            "public class functions {\n" +
            "    public static Column when(Column condition, Object value) { return null; }\n" +
            "    public static Column to_date(Column e) { return null; }\n" +
            "    public static Column last_day(Column e) { return null; }\n" +
            "    public static Column add_months(Column startDate, int numMonths) { return null; }\n" +
            "}\n"
    };

    private static final String SNAP =
            "P.when(P.to_date(#{d:any(org.apache.spark.sql.Column)}).equalTo(P.last_day(#{d})), " +
            "P.last_day(P.add_months(#{d}, #{n:any(int)}))).otherwise(P.add_months(#{d}, #{n}))";

    @Override
    public String getDisplayName() {
        return "Keep Spark 2.4's month-end result for add_months";
    }

    @Override
    public String getDescription() {
        return "From Spark 3.0, add_months no longer returns the last day of the month when the input is the last " +
                "day of its month. Rewrites add_months(d, n) to when(to_date(d).equalTo(last_day(d)), " +
                "last_day(add_months(d, n))).otherwise(add_months(d, n)), which returns 2.4's result.";
    }

    /** True when {@code addMonths} (the unvisited call, at {@code cursor}) is one this recipe rewrites. */
    static boolean isRewritable(J.MethodInvocation addMonths, Cursor cursor) {
        if (!ADD_MONTHS.matches(addMonths) || needsNoChange(cursor)) {
            return false;
        }
        List<Expression> args = addMonths.getArguments();
        return isRepeatableColumn(args.get(0)) && isRepeatableInt(args.get(1)) && ColumnNames.isDiscarded(addMonths, cursor);
    }

    /**
     * The call at {@code cursor} gives 2.4's result already: it is the argument of {@code last_day(...)}
     * (the month is the same in 2.4 and 3.x, so its last day is too), or the {@code otherwise} branch of
     * this recipe's own output. The second case is what keeps the rewrite from re-wrapping its own result.
     */
    static boolean needsNoChange(Cursor cursor) {
        Object parent = cursor.getParentTreeCursor().getValue();
        if (!(parent instanceof J.MethodInvocation)) {
            return false;
        }
        J.MethodInvocation p = (J.MethodInvocation) parent;
        if (LAST_DAY.matches(p)) {
            return true;
        }
        if (!OTHERWISE.matches(p) || !(p.getSelect() instanceof J.MethodInvocation)) {
            return false;
        }
        J.MethodInvocation when = (J.MethodInvocation) p.getSelect();
        if (!WHEN.matches(when) || !(when.getArguments().get(0) instanceof J.MethodInvocation)) {
            return false;
        }
        J.MethodInvocation condition = (J.MethodInvocation) when.getArguments().get(0);
        return EQUAL_TO.matches(condition) &&
                condition.getSelect() instanceof J.MethodInvocation && matchesAny(DATE_CONVERSIONS, (J.MethodInvocation) condition.getSelect()) &&
                condition.getArguments().get(0) instanceof J.MethodInvocation && LAST_DAY.matches((J.MethodInvocation) condition.getArguments().get(0));
    }

    private static boolean isRepeatableColumn(Expression e) {
        if (e instanceof J.Identifier) {
            return true;
        }
        if (!(e instanceof J.MethodInvocation)) {
            return false;
        }
        J.MethodInvocation m = (J.MethodInvocation) e;
        List<Expression> args = m.getArguments();
        if (matchesAny(COLUMN_BY_NAME, m)) {
            return isTypeName(m.getSelect()) && isStringLiteral(args.get(0));
        }
        if (matchesAny(DATASET_COLUMN_BY_NAME, m)) {
            return m.getSelect() instanceof J.Identifier && isStringLiteral(args.get(0));
        }
        if (matchesAny(DATE_CONVERSIONS, m)) {
            return isTypeName(m.getSelect()) && isRepeatableColumn(args.get(0)) &&
                    (args.size() == 1 || isStringLiteral(args.get(1)));
        }
        return false;
    }

    private static boolean isRepeatableInt(Expression e) {
        if (e instanceof J.Unary && ((J.Unary) e).getOperator() == J.Unary.Type.Negative) {
            e = ((J.Unary) e).getExpression();
        }
        return e instanceof J.Literal || e instanceof J.Identifier;
    }

    private static boolean matchesAny(List<MethodMatcher> matchers, J.MethodInvocation m) {
        return matchers.stream().anyMatch(matcher -> matcher.matches(m));
    }

    private static boolean isTypeName(Expression select) {
        return select == null || select instanceof J.Identifier || select instanceof J.FieldAccess;
    }

    private static boolean isStringLiteral(Expression e) {
        return e instanceof J.Literal && ((J.Literal) e).getValue() instanceof String;
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                boolean rewrite = isRewritable(method, getCursor());
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (!rewrite) {
                    return m;
                }
                String prefix;
                if (m.getSelect() != null) {
                    prefix = m.getSelect().printTrimmed(getCursor()) + ".";
                } else if (declaresMethodNamed(ADDED_FUNCTIONS)) {
                    prefix = "functions.";
                    maybeAddImport(FUNCTIONS);
                } else {
                    prefix = "";
                    // Only names not imported yet: AddImport misses an existing import of the
                    // same member (adding functions.* twice, or fully qualifying every to_date).
                    for (String function : ADDED_FUNCTIONS) {
                        if (!importsStatically(function)) {
                            maybeAddImport(FUNCTIONS, function);
                        }
                    }
                }
                return JavaTemplate.builder(SNAP.replace("P.", prefix))
                        .imports(FUNCTIONS)
                        .staticImports(FUNCTIONS + ".*")
                        .javaParser(JavaParser.fromJavaVersion().dependsOn(STUBS))
                        .build()
                        .apply(getCursor(), m.getCoordinates().replace(), m.getArguments().get(0), m.getArguments().get(1));
            }

            private boolean importsStatically(String function) {
                return getCursor().firstEnclosingOrThrow(J.CompilationUnit.class).getImports().stream()
                        .anyMatch(i -> i.isStatic() &&
                                TypeUtils.isOfClassType(i.getQualid().getTarget().getType(), FUNCTIONS) &&
                                (function.equals(i.getQualid().getSimpleName()) || "*".equals(i.getQualid().getSimpleName())));
            }

            /** A method of this name declared in the file would shadow a static import. */
            private boolean declaresMethodNamed(List<String> names) {
                Set<String> declared = new HashSet<>();
                new JavaIsoVisitor<Set<String>>() {
                    @Override
                    public J.MethodDeclaration visitMethodDeclaration(J.MethodDeclaration md, Set<String> acc) {
                        acc.add(md.getSimpleName());
                        return super.visitMethodDeclaration(md, acc);
                    }
                }.visit(getCursor().firstEnclosingOrThrow(J.CompilationUnit.class), declared);
                return names.stream().anyMatch(declared::contains);
            }
        };
    }
}
