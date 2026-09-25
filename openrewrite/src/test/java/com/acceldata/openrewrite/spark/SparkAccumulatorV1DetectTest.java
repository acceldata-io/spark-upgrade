package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkAccumulatorV1DetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkAccumulatorV1Detect());
    }

    @Test
    void flagsJavaSparkContextAccumulator() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.Accumulator;
                        import org.apache.spark.api.java.JavaSparkContext;

                        class Counters {
                            Accumulator<Integer> counter(JavaSparkContext jsc) {
                                return jsc.accumulator(0);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.Accumulator;
                        import org.apache.spark.api.java.JavaSparkContext;

                        class Counters {
                            Accumulator<Integer> counter(JavaSparkContext jsc) {
                                return /*~~(The v1 accumulator API is removed in Spark 3.x; migrate to longAccumulator()/doubleAccumulator() or a custom AccumulatorV2.)~~>*/jsc.accumulator(0);
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
                        class Counters {
                            int accumulator(int x) {
                                return x;
                            }

                            int use() {
                                return accumulator(1);
                            }
                        }
                        """
                )
        );
    }
}
