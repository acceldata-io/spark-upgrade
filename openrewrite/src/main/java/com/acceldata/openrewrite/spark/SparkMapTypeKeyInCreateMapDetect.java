package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

import java.util.List;

/**
 * Java-side counterpart to the Scalafix {@code MapTypeKeyInCreateMapDetect}
 * rule (2026-09-29 coverage review, ported): a {@code MapType}-typed key
 * inside {@code functions.map(...)} -- i.e. one key argument is itself
 * another {@code map(...)} call -- is disallowed by Spark 3.0's analyzer
 * ({@code AnalysisException}, since map keys must support equality/hashing
 * and {@code MapType} doesn't), whereas 2.4 let it through. The varargs
 * {@code Column...} overload was confirmed present via {@code javap}
 * against the real {@code spark-sql_2.11-2.4.8.jar} (2026-09-29).
 *
 * <p>Only matches a key argument that is itself a direct {@code
 * functions.map(...)} call -- a variable holding a {@code MapType} column
 * built elsewhere isn't traced, the same "exact signal over a broad,
 * uncertain one" trade-off {@code SparkSelfJoinAmbiguousDetect} documents
 * for a different rule. Detect-only (Tier 3): the fix is {@code
 * map_entries(...)} plus a restructure of the outer map, a business-logic
 * decision, not a mechanical rewrite.
 */
public class SparkMapTypeKeyInCreateMapDetect extends Recipe {

    private static final MethodMatcher MAP_MATCHER =
            new MethodMatcher("org.apache.spark.sql.functions map(org.apache.spark.sql.Column...)");

    @Override
    public String getDisplayName() {
        return "Detect a MapType-valued key inside functions.map(...)";
    }

    @Override
    public String getDescription() {
        return "A map(...) literal used directly as a key inside another map(...) is disallowed by Spark " +
                "3.0's analyzer (MapType keys can't support equality/hashing), where 2.4 let it through.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (MAP_MATCHER.matches(m)) {
                    List<Expression> args = m.getArguments();
                    for (int i = 0; i < args.size(); i += 2) {
                        Expression key = args.get(i);
                        if (key instanceof J.MethodInvocation && MAP_MATCHER.matches((J.MethodInvocation) key)) {
                            return SearchResult.found(m,
                                    "This map(...)'s key is itself a map(...) literal (MapType); Spark 3.0's " +
                                            "analyzer rejects a MapType key with an AnalysisException.");
                        }
                    }
                }
                return m;
            }
        };
    }
}
