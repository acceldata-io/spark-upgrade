package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkOneHotEncoderEstimatorUsageDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkOneHotEncoderEstimatorUsageDetect());
    }

    @Test
    void flagsOneHotEncoderEstimatorImport() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.ml.feature.OneHotEncoderEstimator;

                        class Pipeline {
                        }
                        """,
                        """
                        /*~~(OneHotEncoderEstimator is renamed to OneHotEncoder in Spark 3.0 -- update the class name (the old OneHotEncoder itself is removed).)~~>*/import org.apache.spark.ml.feature.OneHotEncoderEstimator;

                        class Pipeline {
                        }
                        """
                )
        );
    }

    @Test
    void leavesUnrelatedImportUntouched() {
        rewriteRun(
                java(
                        """
                        class Pipeline {
                        }
                        """
                )
        );
    }
}
