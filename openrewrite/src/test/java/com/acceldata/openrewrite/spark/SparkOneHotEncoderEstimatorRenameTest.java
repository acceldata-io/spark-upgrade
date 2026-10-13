package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkOneHotEncoderEstimatorRenameTest implements RewriteTest {

    // spark-mllib is not on this module's test classpath; a stub of 2.4.8's
    // class gives the references real type attribution.
    static final String ESTIMATOR_STUB =
            "package org.apache.spark.ml.feature;\n" +
            "public class OneHotEncoderEstimator {\n" +
            "    public OneHotEncoderEstimator() {}\n" +
            "    public OneHotEncoderEstimator setInputCols(String[] cols) { return this; }\n" +
            "    public OneHotEncoderEstimator setOutputCols(String[] cols) { return this; }\n" +
            "    public static OneHotEncoderEstimator load(String path) { return null; }\n" +
            "}\n";

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkOneHotEncoderEstimatorRename())
                .parser(JavaParser.fromJavaVersion().dependsOn(ESTIMATOR_STUB));
    }

    @Test
    void renamesImportTypesConstructorsAndStaticCalls() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.ml.feature.OneHotEncoderEstimator;

                        class Features {
                            OneHotEncoderEstimator encoder() {
                                OneHotEncoderEstimator e = new OneHotEncoderEstimator()
                                        .setInputCols(new String[]{"category"})
                                        .setOutputCols(new String[]{"category_vec"});
                                return e;
                            }

                            Object saved(String path) {
                                return OneHotEncoderEstimator.load(path);
                            }

                            Object qualified() {
                                return new org.apache.spark.ml.feature.OneHotEncoderEstimator();
                            }
                        }
                        """,
                        """
                        import org.apache.spark.ml.feature.OneHotEncoder;

                        class Features {
                            OneHotEncoder encoder() {
                                OneHotEncoder e = new OneHotEncoder()
                                        .setInputCols(new String[]{"category"})
                                        .setOutputCols(new String[]{"category_vec"});
                                return e;
                            }

                            Object saved(String path) {
                                return OneHotEncoder.load(path);
                            }

                            Object qualified() {
                                return new OneHotEncoder();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesTheNameInAString() {
        rewriteRun(
                java(
                        """
                        class Features {
                            String stage = "OneHotEncoderEstimator";
                        }
                        """
                )
        );
    }
}
