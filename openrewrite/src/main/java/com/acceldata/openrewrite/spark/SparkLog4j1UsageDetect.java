package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side rule closing a 2026-09-29 coverage-review gap against the core
 * migration guide: Spark switched its own logging from Log4j 1.x to Log4j
 * 2.x in Spark 3.3 (Log4j 1.x reached end-of-life). A customer's own {@code
 * org.apache.log4j.*} usage (a very common direct-logging pattern in
 * pre-3.3 Java Spark apps) needs its own migration to the Log4j 2 API, and
 * any {@code log4j.properties} file needs rewriting to Log4j 2 syntax.
 *
 * <p>Matched on the import statement's own printed text ({@link
 * J.Import#getTypeName()}), the same reason {@code SparkMesosUsageDetect}
 * does: {@code log4j:log4j} is not, and has no reason to be, a dependency
 * of this module, so a resolved-type match would never fire. Detect-only
 * (Tier 3): migrating a whole logging setup (API calls, config file format,
 * appender configuration) is not a mechanical rewrite this recipe can make
 * safely on its own.
 */
public class SparkLog4j1UsageDetect extends Recipe {

    private static final String LOG4J1_PACKAGE = "org.apache.log4j";

    @Override
    public String getDisplayName() {
        return "Detect direct Log4j 1.x usage";
    }

    @Override
    public String getDescription() {
        return "Spark itself moves from Log4j 1.x to Log4j 2.x in Spark 3.3; this application's own direct " +
                "org.apache.log4j usage needs its own migration to the Log4j 2 API and config format.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.Import visitImport(J.Import anImport, ExecutionContext ctx) {
                J.Import i = super.visitImport(anImport, ctx);
                String typeName = i.getTypeName();
                if (typeName != null && (typeName.equals(LOG4J1_PACKAGE) || typeName.startsWith(LOG4J1_PACKAGE + "."))) {
                    return SearchResult.found(i,
                            "Log4j 1.x reached end-of-life and Spark itself moved to Log4j 2.x in Spark 3.3; " +
                                    "this application's own logging code needs its own migration.");
                }
                return i;
            }
        };
    }
}
