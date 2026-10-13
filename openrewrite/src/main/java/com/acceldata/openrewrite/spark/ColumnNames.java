package com.acceldata.openrewrite.spark;

import org.openrewrite.Cursor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.TypeUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Spark names an unaliased column after its expression ({@code add_months(d, 1)},
 * {@code array()}). A rewrite that changes a Column expression can change that
 * name, so the rewrites that do ({@link SparkAddMonthsRewrite}, {@link
 * SparkEmptyCollectionTypeRewrite}) fire only where the name is thrown away.
 */
final class ColumnNames {

    private static final String COLUMN = "org.apache.spark.sql.Column";
    private static final String DATASET = "org.apache.spark.sql.Dataset";

    private static final List<MethodMatcher> RENAMES = Arrays.asList(
            new MethodMatcher(COLUMN + " as(..)"),
            new MethodMatcher(COLUMN + " alias(..)"),
            new MethodMatcher(COLUMN + " name(..)"));
    private static final MethodMatcher WITH_COLUMN = new MethodMatcher(DATASET + " withColumn(java.lang.String, " + COLUMN + ")");
    private static final List<MethodMatcher> CONDITIONS = Arrays.asList(
            new MethodMatcher(DATASET + " filter(" + COLUMN + ")"),
            new MethodMatcher(DATASET + " where(" + COLUMN + ")"));

    private ColumnNames() {
    }

    /**
     * True when the expression {@code child} (at {@code cursor}, unvisited) ends up where its
     * generated name is discarded: walking up through the Column expression containing it, that
     * expression is renamed by {@code .as}/{@code .alias}/{@code .name}, is the value of {@code
     * withColumn}, or is a {@code filter}/{@code where} condition.
     */
    static boolean isDiscarded(J child, Cursor cursor) {
        Cursor parentCursor = cursor.getParentTreeCursor();
        Object parent = parentCursor.getValue();
        if (parent instanceof J.Parentheses) {
            return isDiscarded((J) parent, parentCursor);
        }
        if (!(parent instanceof J.MethodInvocation)) {
            return false;
        }
        J.MethodInvocation p = (J.MethodInvocation) parent;
        if (RENAMES.stream().anyMatch(m -> m.matches(p))) {
            return p.getSelect() != null && p.getSelect().getId().equals(child.getId());
        }
        if (WITH_COLUMN.matches(p)) {
            return p.getArguments().get(1).getId().equals(child.getId());
        }
        if (CONDITIONS.stream().anyMatch(m -> m.matches(p))) {
            return true;
        }
        return TypeUtils.isOfClassType(p.getType(), COLUMN) && isDiscarded(p, parentCursor);
    }
}
