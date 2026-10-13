package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code AddMonthsSnapDetect} rule:
 * {@code functions.add_months(date, numMonths)} no longer snaps the result to
 * the last day of the month in Spark 3.0 onward when the input date is already
 * the last day of its month -- e.g. {@code add_months('2019-02-28', 1)} returns
 * {@code 2019-03-28} now, not {@code 2019-03-31}. The {@code (Column, int)}
 * overload was confirmed present via {@code javap} against the real {@code
 * spark-sql_2.11-2.4.8.jar} (2026-09-29).
 *
 * <p>Reports only the calls {@link SparkAddMonthsRewrite} does not rewrite
 * (an argument that is not safe to evaluate twice, or a result whose
 * auto-generated column name is kept), so each call is reported by exactly
 * one of the two. A call inside {@code last_day(...)} is reported by neither:
 * its result is the same in both versions. Tier 3, detect-only.
 */
public class SparkAddMonthsUsageWarn extends Recipe {

    @Override
    public String getDisplayName() {
        return "Detect add_months(...) calls the tool cannot rewrite";
    }

    @Override
    public String getDescription() {
        return "add_months(...) no longer returns the last day of the month from Spark 3.0 when the input is the " +
                "last day of its month. Reports the calls the tool cannot rewrite safely.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (SparkAddMonthsRewrite.ADD_MONTHS.matches(m) && !SparkAddMonthsRewrite.needsNoChange(getCursor()) &&
                        !SparkAddMonthsRewrite.isRewritable(method, getCursor())) {
                    return SearchResult.found(m,
                            "add_months(...) no longer returns the last day of the month for a month-end input in " +
                                    "Spark 3.0. If 2.4's result matters, rewrite it as when(to_date(d).equalTo(last_day(d)), " +
                                    "last_day(add_months(d, n))).otherwise(add_months(d, n)), and alias it if its column " +
                                    "name matters.");
                }
                return m;
            }
        };
    }
}
