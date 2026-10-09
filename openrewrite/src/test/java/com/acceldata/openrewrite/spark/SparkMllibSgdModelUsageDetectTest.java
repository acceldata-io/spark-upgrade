package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkMllibSgdModelUsageDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkMllibSgdModelUsageDetect());
    }

    @Test
    void flagsLogisticRegressionWithSgdImport() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;

                        class Trainer {
                        }
                        """,
                        """
                        /*~~(org.apache.spark.mllib.classification.LogisticRegressionWithSGD is removed in Spark 3.0 -- migrate to its spark.ml equivalent.)~~>*/import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;

                        class Trainer {
                        }
                        """
                )
        );
    }

    @Test
    void leavesSparkMlImportUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.ml.classification.LogisticRegression;

                        class Trainer {
                        }
                        """
                )
        );
    }
}
