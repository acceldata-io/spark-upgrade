package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side rule closing a 2026-09-29 coverage-review gap against the core
 * migration guide (ported from the Scalafix {@code MesosRemovedWarn}
 * rule's intent): Mesos as a resource manager is deprecated in Spark 3.2
 * and removed in Spark 4.0. Any code referencing the {@code
 * org.apache.spark.scheduler.cluster.mesos} package means the application
 * is deployed against (or has cluster-integration code for) Mesos.
 *
 * <p>Matched on the import statement's own printed text ({@link
 * J.Import#getTypeName()}), not a resolved type, deliberately: {@code
 * spark-mesos} is a separate published module this project has never had a
 * reason to add as a test/runtime dependency, so a type-resolution-based
 * match (the way {@code SparkCalendarIntervalUsageDetect} matches {@code
 * CalendarInterval}) would silently never fire without that jar on the
 * classpath. {@code getTypeName()} is derived purely from the import's own
 * syntax, so it works regardless of what's resolvable. Detect-only (Tier
 * 3): whether to migrate off Mesos entirely is an infrastructure decision,
 * not a code rewrite.
 */
public class SparkMesosUsageDetect extends Recipe {

    private static final String MESOS_PACKAGE = "org.apache.spark.scheduler.cluster.mesos";

    @Override
    public String getDisplayName() {
        return "Detect Mesos cluster-manager usage";
    }

    @Override
    public String getDescription() {
        return "Mesos as a Spark resource manager is deprecated in Spark 3.2 and removed in Spark 4.0; this " +
                "import means the application integrates with Mesos.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.Import visitImport(J.Import anImport, ExecutionContext ctx) {
                J.Import i = super.visitImport(anImport, ctx);
                String typeName = i.getTypeName();
                if (typeName != null && (typeName.equals(MESOS_PACKAGE) || typeName.startsWith(MESOS_PACKAGE + "."))) {
                    return SearchResult.found(i,
                            "Mesos is deprecated in Spark 3.2 and removed in Spark 4.0 -- this application " +
                                    "integrates with Mesos as a cluster manager.");
                }
                return i;
            }
        };
    }
}
