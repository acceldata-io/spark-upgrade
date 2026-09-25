package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.TypeTree;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code ExecutorPluginWarn} rule:
 * {@code org.apache.spark.ExecutorPlugin} was removed in Spark 3.0 in favor
 * of {@code org.apache.spark.api.plugin.SparkPlugin}; a removed API
 * implemented anywhere is a hard compile break. {@code ExecutorPlugin} is
 * already a plain Java interface in 2.4.8 (verified via {@code javap}
 * against the real {@code spark-core_2.11-2.4.8.jar}, 2026-09-24 -- it's
 * compiled from {@code ExecutorPlugin.java}, not a Scala trait), so a Java
 * class implementing it is exactly as reachable as a Scala one.
 *
 * <p>Matches the type reference in an {@code implements} clause directly
 * (not an import), since Java code can reference the type by its
 * fully-qualified name inline without ever importing it. Marks the whole
 * class declaration rather than the single {@code implements} entry -- a
 * coarser anchor, but a simpler and equally-informative one for a finding
 * whose whole point is "this class can no longer compile as written."
 */
public class SparkExecutorPluginDetect extends Recipe {

    private static final String FQN = "org.apache.spark.ExecutorPlugin";

    @Override
    public String getDisplayName() {
        return "Detect a class implementing the removed ExecutorPlugin interface";
    }

    @Override
    public String getDescription() {
        return "org.apache.spark.ExecutorPlugin was removed in Spark 3.0 in favor of " +
                "org.apache.spark.api.plugin.SparkPlugin; a removed API used anywhere is a hard compile break.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.ClassDeclaration visitClassDeclaration(J.ClassDeclaration classDecl, ExecutionContext ctx) {
                J.ClassDeclaration c = super.visitClassDeclaration(classDecl, ctx);
                if (c.getImplements() != null) {
                    for (TypeTree impl : c.getImplements()) {
                        if (isExecutorPlugin(impl.getType())) {
                            return SearchResult.found(
                                    c,
                                    "ExecutorPlugin was removed in Spark 3.0; migrate to " +
                                            "org.apache.spark.api.plugin.SparkPlugin."
                            );
                        }
                    }
                }
                return c;
            }

            private boolean isExecutorPlugin(JavaType type) {
                return type instanceof JavaType.FullyQualified
                        && FQN.equals(((JavaType.FullyQualified) type).getFullyQualifiedName());
            }
        };
    }
}
