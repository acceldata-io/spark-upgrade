package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code GroupByKeyWarn} rule: for a
 * non-struct key, {@code groupByKey(...).count()}, {@code .agg(...)} and
 * {@code .reduceGroups(...)} name the key column {@code "value"} in 2.4 and
 * {@code "key"} from Spark 3.0. All three go through Spark's {@code
 * KeyValueGroupedDataset.aggUntyped}, which names the column ({@code
 * reduceGroups} is an {@code agg} of a {@code ReduceAggregator}). {@code
 * mapGroups}/{@code flatMapGroups}/{@code keys} are unaffected and not
 * matched. Probed on 2.4.8 and 3.5.5, and the config
 * below restores {@code "value"} for all three.
 *
 * <p>Tier 2: {@code spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue=true}
 * restores 2.4's name. Still fuzzy about the key type: a struct key was never
 * renamed, and this recipe does not resolve the key's type.
 */
public class SparkGroupByKeyCountWarn extends Recipe {

    private static final MethodMatcher COUNT =
            new MethodMatcher("org.apache.spark.sql.KeyValueGroupedDataset count()");
    private static final MethodMatcher AGG =
            new MethodMatcher("org.apache.spark.sql.KeyValueGroupedDataset agg(..)");
    private static final MethodMatcher REDUCE_GROUPS =
            new MethodMatcher("org.apache.spark.sql.KeyValueGroupedDataset reduceGroups(..)");

    @Override
    public String getDisplayName() {
        return "Warn on groupByKey(...).count()/agg(...)/reduceGroups(...)'s key-column naming change";
    }

    @Override
    public String getDescription() {
        return "For a non-struct key, groupByKey(...).count(), .agg(...) and .reduceGroups(...) name the key " +
                "column \"value\" in Spark 2.4 and \"key\" from Spark 3.0. " +
                "spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue=true restores \"value\".";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (COUNT.matches(m) || AGG.matches(m) || REDUCE_GROUPS.matches(m)) {
                    return SearchResult.found(
                            m,
                            "For a non-struct key, this result's key column is named \"key\" from Spark 3.0, not " +
                                    "2.4's \"value\"; spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue=true " +
                                    "restores \"value\"."
                    );
                }
                return m;
            }
        };
    }
}
