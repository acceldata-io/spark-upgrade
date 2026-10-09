package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

import java.util.Arrays;
import java.util.List;

/**
 * Java-side rule closing a 2026-09-29 coverage-review gap against the MLlib
 * migration guide: the SGD-optimizer-based model classes -- {@code
 * LogisticRegressionWithSGD}, {@code LinearRegressionWithSGD}, {@code
 * RidgeRegressionWithSGD}, {@code LassoWithSGD} -- are removed in Spark
 * 3.0 in favor of their {@code spark.ml} equivalents (e.g. {@code
 * org.apache.spark.ml.classification.LogisticRegression}).
 *
 * <p>Matched on the import statement's own printed text ({@link
 * J.Import#getTypeName()}), the same reason {@code SparkMesosUsageDetect}
 * does: {@code spark-mllib} is not a dependency of this module, so a
 * resolved-type match would never fire. Detect-only (Tier 3): each class
 * has a differently-shaped {@code spark.ml} replacement (a different
 * package, a {@code Estimator}/{@code Transformer} pipeline shape instead
 * of a static {@code train(...)} call), so this is a rewrite a customer
 * needs to design, not a mechanical one.
 */
public class SparkMllibSgdModelUsageDetect extends Recipe {

    private static final List<String> SGD_MODEL_FQNS = Arrays.asList(
            "org.apache.spark.mllib.classification.LogisticRegressionWithSGD",
            "org.apache.spark.mllib.regression.LinearRegressionWithSGD",
            "org.apache.spark.mllib.regression.RidgeRegressionWithSGD",
            "org.apache.spark.mllib.regression.LassoWithSGD"
    );

    @Override
    public String getDisplayName() {
        return "Detect a removed SGD-based MLlib model class";
    }

    @Override
    public String getDescription() {
        return "LogisticRegressionWithSGD/LinearRegressionWithSGD/RidgeRegressionWithSGD/LassoWithSGD are " +
                "removed in Spark 3.0; migrate to the corresponding spark.ml Estimator.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.Import visitImport(J.Import anImport, ExecutionContext ctx) {
                J.Import i = super.visitImport(anImport, ctx);
                String typeName = i.getTypeName();
                if (typeName != null && SGD_MODEL_FQNS.contains(typeName)) {
                    return SearchResult.found(i,
                            typeName + " is removed in Spark 3.0 -- migrate to its spark.ml equivalent.");
                }
                return i;
            }
        };
    }
}
