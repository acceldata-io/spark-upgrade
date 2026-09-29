package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkProcessingTimeDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkProcessingTimeDetect());
    }

    @Test
    void rewritesProcessingTimeApplyToTriggerProcessingTime() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.streaming.ProcessingTime;
                        import org.apache.spark.sql.streaming.Trigger;

                        class Streams {
                            Trigger every5s() {
                                return ProcessingTime.apply(5000);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.streaming.Trigger;

                        class Streams {
                            Trigger every5s() {
                                return Trigger.ProcessingTime(5000);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void rewritesProcessingTimeCreateToTriggerProcessingTime() {
        rewriteRun(
                java(
                        """
                        import java.util.concurrent.TimeUnit;
                        import org.apache.spark.sql.streaming.ProcessingTime;
                        import org.apache.spark.sql.streaming.Trigger;

                        class Streams {
                            Trigger every5s() {
                                return ProcessingTime.create(5, TimeUnit.SECONDS);
                            }
                        }
                        """,
                        """
                        import java.util.concurrent.TimeUnit;
                        import org.apache.spark.sql.streaming.Trigger;

                        class Streams {
                            Trigger every5s() {
                                return Trigger.ProcessingTime(5, TimeUnit.SECONDS);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesTriggerProcessingTimeUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.streaming.Trigger;

                        class Streams {
                            Trigger every5s() {
                                return Trigger.ProcessingTime(5000);
                            }
                        }
                        """
                )
        );
    }
}
