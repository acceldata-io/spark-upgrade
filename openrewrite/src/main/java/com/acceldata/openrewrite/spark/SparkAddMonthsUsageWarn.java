package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code AddMonthsSnapDetect} rule
 * (2026-09-29 coverage review, ported): {@code functions.add_months(date,
 * numMonths)} no longer snaps the result to the last day of the month in
 * Spark 3.0 onward when the input date is already the last day of its
 * month -- e.g. {@code add_months('2019-02-28', 1)} returns
 * {@code 2019-03-28} now, not {@code 2019-03-31}. The {@code
 * (Column, int)} overload was confirmed present via {@code javap} against
 * the real {@code spark-sql_2.11-2.4.8.jar} (2026-09-29).
 *
 * <p>Fuzzy, by necessity (Tier 3, detect-only): flags every {@code
 * add_months} call regardless of whether the input date is ever actually a
 * month-end value, the same posture {@code SparkGroupByKeyCountWarn} takes
 * for a call this recipe can't narrow further without evaluating the input
 * expression.
 */
public class SparkAddMonthsUsageWarn extends Recipe {

    private static final MethodMatcher ADD_MONTHS_MATCHER =
            new MethodMatcher("org.apache.spark.sql.functions add_months(org.apache.spark.sql.Column, int)");

    @Override
    public String getDisplayName() {
        return "Detect functions.add_months(...) usage";
    }

    @Override
    public String getDescription() {
        return "add_months(...) no longer snaps to the last day of the month from Spark 3.0 onward when the " +
                "input date already is one -- review any input that may land on a month-end date.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (ADD_MONTHS_MATCHER.matches(m)) {
                    return SearchResult.found(m,
                            "add_months(...)'s month-end snapping behavior changed in Spark 3.0 -- review " +
                                    "whether the input date can land on the last day of a month.");
                }
                return m;
            }
        };
    }
}
