package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.TypeUtils;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side rule closing a 2026-09-29 coverage-review gap against the MLlib
 * migration guide: the old {@code
 * org.apache.spark.ml.feature.OneHotEncoder} is removed in Spark 3.0, and
 * {@code OneHotEncoderEstimator} (the multi-column-capable replacement
 * introduced in 2.3) is renamed to take over the {@code OneHotEncoder} name
 * itself. Pre-3.0 Java code that already migrated to {@code
 * OneHotEncoderEstimator} (the recommended API since 2.3) needs only the
 * class name updated -- the API itself is unchanged across the rename.
 *
 * <p>{@link SparkOneHotEncoderEstimatorRename} renames every reference whose
 * type resolves. This reports the import when it does not (spark-mllib not on
 * the classpath the analysis read), matched on the import's printed text
 * ({@link J.Import#getTypeName()}), the same way {@code SparkMesosUsageDetect} does.
 */
public class SparkOneHotEncoderEstimatorUsageDetect extends Recipe {

    private static final String FQN = "org.apache.spark.ml.feature.OneHotEncoderEstimator";

    @Override
    public String getDisplayName() {
        return "Detect OneHotEncoderEstimator usage";
    }

    @Override
    public String getDescription() {
        return "OneHotEncoderEstimator is renamed to OneHotEncoder in Spark 3.0 (the old OneHotEncoder is " +
                "removed); the API is otherwise unchanged, so this is a plain class-name update.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.Import visitImport(J.Import anImport, ExecutionContext ctx) {
                J.Import i = super.visitImport(anImport, ctx);
                if (FQN.equals(i.getTypeName()) && !TypeUtils.isOfClassType(i.getQualid().getType(), FQN)) {
                    return SearchResult.found(i,
                            "OneHotEncoderEstimator is renamed to OneHotEncoder in Spark 3.0 -- update the " +
                                    "class name (the old OneHotEncoder itself is removed).");
                }
                return i;
            }
        };
    }
}
