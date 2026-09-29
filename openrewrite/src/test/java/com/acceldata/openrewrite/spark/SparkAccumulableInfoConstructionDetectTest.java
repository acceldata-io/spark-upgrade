package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkAccumulableInfoConstructionDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkAccumulableInfoConstructionDetect());
    }

    @Test
    void flagsAccumulableInfoApply() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.scheduler.AccumulableInfo;

                        class Metrics {
                            AccumulableInfo build() {
                                return AccumulableInfo.apply(1L, "counter", "5");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.scheduler.AccumulableInfo;

                        class Metrics {
                            AccumulableInfo build() {
                                return /*~~(AccumulableInfo.apply(...) is removed in Spark 3.0 -- this type is internal plumbing, not meant to be constructed directly.)~~>*/AccumulableInfo.apply(1L, "counter", "5");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesUnrelatedMethodCallUntouched() {
        rewriteRun(
                java(
                        """
                        class Metrics {
                            String build() {
                                return String.valueOf(1L);
                            }
                        }
                        """
                )
        );
    }
}
