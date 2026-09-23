package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;

/**
 * Java-side counterpart to the Scalafix {@code UnionRewrite} rule: {@code
 * Dataset#unionAll} is deprecated in favor of {@code Dataset#union}, an exact
 * rename with identical semantics. Both the old and new methods are verified
 * present in the real {@code spark-sql_2.11-2.4.8.jar} (via {@code javap},
 * 2026-09-21 -- COVERAGE.md Part 9 already recorded this for the Scala side;
 * the same bytecode is called identically from Java, so the fact transfers
 * without re-deriving it), so this rewrite is valid pre-migration too and
 * cannot break the pre-3.5.5 build.
 *
 * <p>Tier-1-*shaped* on the Scala side, but every recipe in this jar is
 * reported at Tier 3 by {@code JavaRuleRegistry} until individually audited
 * (spark-migrate-java-openrewrite-integration-2026-09-18.md SS5 item 4) --
 * this analyze-only pass never runs codegen, so the tier label here has no
 * auto-apply consequence yet.
 */
public class SparkUnionAllRename extends Recipe {

    private static final MethodMatcher MATCHER =
            new MethodMatcher("org.apache.spark.sql.Dataset unionAll(org.apache.spark.sql.Dataset)");

    @Override
    public String getDisplayName() {
        return "Rename Dataset#unionAll to Dataset#union";
    }

    @Override
    public String getDescription() {
        return "unionAll is deprecated; union is the exact, semantics-preserving equivalent.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (MATCHER.matches(m)) {
                    return m.withName(m.getName().withSimpleName("union"));
                }
                return m;
            }
        };
    }
}
