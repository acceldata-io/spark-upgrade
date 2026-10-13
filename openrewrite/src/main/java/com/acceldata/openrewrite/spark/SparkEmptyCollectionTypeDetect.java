package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
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
 * <p>Reports the calls {@link SparkEmptyCollectionTypeRewrite} does not
 * rewrite, those whose generated column name survives. Feeds {@code
 * spark.sql.legacy.createEmptyCollectionUsingStringType} -- Tier 2, cross-linked
 * into the {@code LegacyConfigRegistry} entry's {@code detectionRuleIds} so
 * Phase B (spark-migrate-cli's {@code PhaseBRunner}) injects it.
 */
public class SparkEmptyCollectionTypeDetect extends Recipe {

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
                boolean report = SparkEmptyCollectionTypeRewrite.isUntypedEmptyCollection(method, getCursor()) &&
                        !SparkEmptyCollectionTypeRewrite.isRewritable(method, getCursor());
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (report) {
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
