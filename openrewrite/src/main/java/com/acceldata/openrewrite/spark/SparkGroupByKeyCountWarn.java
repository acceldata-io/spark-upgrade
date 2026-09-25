package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code GroupByKeyWarn} rule:
 * {@code Dataset#groupByKey}'s grouping attribute is named {@code "value"}
 * for a non-struct key in 2.4, {@code "key"} from Spark 3.0 (preserved under
 * {@code spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue}).
 * Deliberately fuzzy/Tier 3, exactly like the Scala rule: flags every
 * {@code groupByKey} call regardless of whether the key type is actually
 * non-struct, since that needs type inference this rule doesn't do. Both
 * Java-friendly {@code groupByKey} overloads
 * ({@code MapFunction}-based and {@code Function1}-based) verified present
 * in the real {@code spark-sql_2.11-2.4.8.jar} via {@code javap}
 * (2026-09-24).
 */
public class SparkGroupByKeyCountWarn extends Recipe {

    private static final MethodMatcher MATCHER =
            new MethodMatcher("org.apache.spark.sql.Dataset groupByKey(..)");

    @Override
    public String getDisplayName() {
        return "Warn on Dataset#groupByKey's key-column naming change";
    }

    @Override
    public String getDescription() {
        return "In Spark 2.4 and below, Dataset.groupByKey's grouped dataset names its key attribute \"value\" " +
                "if the key is a non-struct type; since Spark 3.0 it's named \"key\" instead (preserved under " +
                "spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue, default false). This linter rule is " +
                "fuzzy: it flags every groupByKey call regardless of key type.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (MATCHER.matches(m)) {
                    return SearchResult.found(
                            m,
                            "groupByKey's grouping attribute is named \"key\" from Spark 3.0 onward, not 2.4's " +
                                    "\"value\", for a non-struct key. Review every \"value\" column reference derived " +
                                    "from this pipeline."
                    );
                }
                return m;
            }
        };
    }
}
