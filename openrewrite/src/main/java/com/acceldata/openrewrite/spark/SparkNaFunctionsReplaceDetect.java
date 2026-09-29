package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code NaFunctionsNameMatchDetect}
 * rule (2026-09-29 coverage review, ported): {@code
 * DataFrameNaFunctions#replace(...)} stops doing an exact string match on
 * column names in Spark 3.2 -- a dotted/backtick-quoted name that used to
 * match now throws {@code AnalysisException}/{@code IllegalArgumentException}
 * instead of silently ignoring the column. All four Java-callable overloads
 * (String/String[] column selector, java.util.Map/scala.collection.immutable.Map
 * replacement) confirmed present via {@code javap} against the real
 * {@code spark-sql_2.11-2.4.8.jar} (2026-09-29).
 *
 * <p>Detect-only (Tier 3): flags every {@code .replace(...)} call on {@code
 * DataFrameNaFunctions} regardless of whether its column-name argument is
 * actually dotted/quoted, the same "flag every call, let a human judge"
 * posture {@code SparkGroupByKeyCountWarn} already takes for a call shape
 * this recipe can't narrow further without resolving the literal column
 * name against the DataFrame's real schema.
 */
public class SparkNaFunctionsReplaceDetect extends Recipe {

    private static final MethodMatcher REPLACE_MATCHER =
            new MethodMatcher("org.apache.spark.sql.DataFrameNaFunctions replace(..)");

    @Override
    public String getDisplayName() {
        return "Detect DataFrameNaFunctions#replace(...) usage";
    }

    @Override
    public String getDescription() {
        return "na().replace(...) stops doing an exact string match on column names in Spark 3.2; a " +
                "dotted or backtick-quoted name that used to match can now throw instead of being ignored.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (REPLACE_MATCHER.matches(m)) {
                    return SearchResult.found(m,
                            "na().replace(...)'s column-name matching is stricter from Spark 3.2 onward -- " +
                                    "review the column name argument for dots/backticks.");
                }
                return m;
            }
        };
    }
}
