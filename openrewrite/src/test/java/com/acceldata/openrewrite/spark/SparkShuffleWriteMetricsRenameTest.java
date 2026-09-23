package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkShuffleWriteMetricsRenameTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkShuffleWriteMetricsRename());
    }

    @Test
    void rewritesAllThreeRemovedAccessors() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.executor.ShuffleWriteMetrics;

                        class Report {
                            long summarize(ShuffleWriteMetrics m) {
                                return m.shuffleBytesWritten() + m.shuffleWriteTime() + m.shuffleRecordsWritten();
                            }
                        }
                        """,
                        """
                        import org.apache.spark.executor.ShuffleWriteMetrics;

                        class Report {
                            long summarize(ShuffleWriteMetrics m) {
                                return m.bytesWritten() + m.writeTime() + m.recordsWritten();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAlreadyCorrectAccessorsUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.executor.ShuffleWriteMetrics;

                        class Report {
                            long summarize(ShuffleWriteMetrics m) {
                                return m.bytesWritten() + m.writeTime() + m.recordsWritten();
                            }
                        }
                        """
                )
        );
    }
}
