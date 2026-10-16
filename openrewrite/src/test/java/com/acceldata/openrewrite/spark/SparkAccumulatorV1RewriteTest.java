package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkAccumulatorV1RewriteTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkAccumulatorV1Rewrite());
    }

    @Test
    void integerCounterBecomesALongAccumulator() {
        // The user trial's billing-java shape: a local Integer counter used in a lambda and read once.
        rewriteRun(
                java(
                        """
                        import java.util.Map;
                        import org.apache.spark.Accumulator;
                        import org.apache.spark.api.java.JavaRDD;
                        import org.apache.spark.api.java.JavaSparkContext;

                        class Job {
                            void run(JavaSparkContext jsc, JavaRDD<String> lines, Map<String, Object> summary) {
                                Accumulator<Integer> bad = jsc.accumulator(0);
                                lines.foreach(l -> { if (l.isEmpty()) bad.add(1); });
                                int n = bad.value();
                                summary.put("bad", bad.value());
                            }
                        }
                        """,
                        """
                        import java.util.Map;
                        import org.apache.spark.api.java.JavaRDD;
                        import org.apache.spark.api.java.JavaSparkContext;
                        import org.apache.spark.util.LongAccumulator;

                        class Job {
                            void run(JavaSparkContext jsc, JavaRDD<String> lines, Map<String, Object> summary) {
                                LongAccumulator bad = jsc.sc().longAccumulator();
                                lines.foreach(l -> { if (l.isEmpty()) bad.add(1); });
                                int n = bad.value().intValue();
                                summary.put("bad", bad.value().intValue());
                            }
                        }
                        """
                )
        );
    }

    @Test
    void namedLongAndDoubleFields() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.Accumulator;
                        import org.apache.spark.api.java.JavaSparkContext;

                        class Job {
                            private final Accumulator<Long> rows;
                            private Accumulator<Double> total = null;

                            Job(JavaSparkContext jsc) {
                                rows = null;
                            }

                            double run(JavaSparkContext jsc) {
                                Accumulator<Double> amount = jsc.doubleAccumulator(0.0, "amount");
                                Accumulator<Long> seen = jsc.accumulator(0L, "seen");
                                seen.add(2L);
                                amount.add(1.5);
                                return amount.value() + seen.value();
                            }
                        }
                        """,
                        """
                        import org.apache.spark.Accumulator;
                        import org.apache.spark.api.java.JavaSparkContext;
                        import org.apache.spark.util.DoubleAccumulator;
                        import org.apache.spark.util.LongAccumulator;

                        class Job {
                            private final Accumulator<Long> rows;
                            private Accumulator<Double> total = null;

                            Job(JavaSparkContext jsc) {
                                rows = null;
                            }

                            double run(JavaSparkContext jsc) {
                                DoubleAccumulator amount = jsc.sc().doubleAccumulator("amount");
                                LongAccumulator seen = jsc.sc().longAccumulator("seen");
                                seen.add(2L);
                                amount.add(1.5);
                                return amount.value() + seen.value();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesWhatIsNotExact() {
        // A non-zero start, a variable passed on, localValue(), an Integer value() used as a receiver,
        // a non-private field, and a call that initialises nothing: SparkAccumulatorV1Detect reports them.
        rewriteRun(
                java(
                        """
                        import org.apache.spark.Accumulator;
                        import org.apache.spark.api.java.JavaSparkContext;

                        class Job {
                            Accumulator<Integer> shared;

                            void run(JavaSparkContext jsc) {
                                Accumulator<Integer> a = jsc.accumulator(5);
                                Accumulator<Integer> b = jsc.accumulator(0);
                                use(b);
                                Accumulator<Integer> c = jsc.accumulator(0);
                                c.localValue();
                                Accumulator<Integer> d = jsc.accumulator(0);
                                String s = d.value().toString();
                                shared = jsc.accumulator(0);
                            }

                            void use(Accumulator<Integer> acc) {
                            }
                        }
                        """
                )
        );
    }
}
