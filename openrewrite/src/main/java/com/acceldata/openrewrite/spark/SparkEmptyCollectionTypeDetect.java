package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code EmptyCollectionTypeDetect}
 * rule: {@code functions.array()}/{@code functions.map()} called with no
 * arguments infers {@code NullType} element(s) in Spark 3.0, where 2.4
 * inferred {@code StringType}. Purely syntactic and exact.
 *
 * <p>Deliberately targets the {@code Column...} varargs overloads, not the
 * {@code scala.collection.Seq}-based ones -- verified via {@code javap}
 * against the real {@code spark-sql_2.11-2.4.8.jar} (2026-09-21) that {@code
 * array}/{@code map} have a Java-friendly {@code Column...} overload while
 * {@code grouping_id} does not (only {@code Seq}-typed overloads exist),
 * which is why {@code grouping_id} was dropped from this initial set rather
 * than built on an assumption -- the same "verify Java-reachability before
 * writing" discipline the integration doc's SS1.2/SS9 already flagged for UDF
 * registration. A zero-argument varargs call parses as a single {@code
 * J.Empty} argument, confirmed against a real spike this session.
 *
 * <p>Detect-only. Feeds {@code spark.sql.legacy.createEmptyCollectionUsingStringType},
 * reusable as-is from {@code LegacyConfigRegistry} once Phase B is wired for
 * Java (out of scope this pass).
 */
public class SparkEmptyCollectionTypeDetect extends Recipe {

    private static final MethodMatcher ARRAY_MATCHER =
            new MethodMatcher("org.apache.spark.sql.functions array(org.apache.spark.sql.Column...)");
    private static final MethodMatcher MAP_MATCHER =
            new MethodMatcher("org.apache.spark.sql.functions map(org.apache.spark.sql.Column...)");

    @Override
    public String getDisplayName() {
        return "Detect no-argument array()/map() calls";
    }

    @Override
    public String getDescription() {
        return "array()/map() called with no arguments infers NullType element(s) in Spark 3.0, " +
                "not 2.4's StringType.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                boolean isEmptyCall = m.getArguments().size() == 1 && m.getArguments().get(0) instanceof J.Empty;
                if (isEmptyCall && (ARRAY_MATCHER.matches(m) || MAP_MATCHER.matches(m))) {
                    return SearchResult.found(
                            m,
                            m.getSimpleName() + "() called with no arguments infers NullType element(s) " +
                                    "in Spark 3.0, not 2.4's StringType."
                    );
                }
                return m;
            }
        };
    }
}
