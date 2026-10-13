package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;

/**
 * {@code org.apache.spark.ml.feature.OneHotEncoderEstimator} is removed in
 * Spark 3.0, where {@code OneHotEncoder} becomes the same estimator under the
 * old name. {@code javap} (2026-10-13): 3.5.5's {@code OneHotEncoder} extends
 * {@code Estimator<OneHotEncoderModel>} with every constructor, setter, getter
 * and param of 2.4.8's {@code OneHotEncoderEstimator} (plus single-column
 * {@code setInputCol}/{@code setOutputCol}), and the same defaults ({@code
 * dropLast=true}, {@code handleInvalid=error}); a fit and transform gives the
 * same vectors on both (Docker probe). So every type reference, constructor
 * and import is renamed. The output needs 3.x: 2.4.8's {@code OneHotEncoder}
 * is the old Transformer.
 *
 * <p>Needs resolved types (spark-mllib on the classpath);
 * {@link SparkOneHotEncoderEstimatorUsageDetect} reports an import it could
 * not resolve.
 */
public class SparkOneHotEncoderEstimatorRename extends Recipe {

    static final String ESTIMATOR = "org.apache.spark.ml.feature.OneHotEncoderEstimator";

    @Override
    public String getDisplayName() {
        return "Rename OneHotEncoderEstimator to OneHotEncoder";
    }

    @Override
    public String getDescription() {
        return "OneHotEncoderEstimator is removed in Spark 3.0; OneHotEncoder is the same estimator under the old name.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new ChangeType(ESTIMATOR, "org.apache.spark.ml.feature.OneHotEncoder", false).getVisitor();
    }
}
