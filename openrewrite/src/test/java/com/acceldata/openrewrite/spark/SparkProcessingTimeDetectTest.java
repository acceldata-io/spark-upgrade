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
    void retypesDeclaredProcessingTimeLocal() {
        rewriteRun(
                java(
                        """
                        import java.util.concurrent.TimeUnit;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.streaming.DataStreamWriter;
                        import org.apache.spark.sql.streaming.ProcessingTime;

                        class Streams {
                            DataStreamWriter<Row> every10s(DataStreamWriter<Row> writer) {
                                ProcessingTime t = ProcessingTime.create(10, TimeUnit.SECONDS);
                                return writer.trigger(t);
                            }
                        }
                        """,
                        """
                        import java.util.concurrent.TimeUnit;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.streaming.DataStreamWriter;
                        import org.apache.spark.sql.streaming.Trigger;

                        class Streams {
                            DataStreamWriter<Row> every10s(DataStreamWriter<Row> writer) {
                                Trigger t = Trigger.ProcessingTime(10, TimeUnit.SECONDS);
                                return writer.trigger(t);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void rewritesEveryOverloadAndDeclarationShape() {
        rewriteRun(
                java(
                        """
                        import java.util.ArrayList;
                        import java.util.List;
                        import org.apache.spark.sql.streaming.ProcessingTime;
                        import scala.concurrent.duration.Duration;

                        class Streams {
                            private final ProcessingTime fixed = new ProcessingTime(5000);
                            private final List<ProcessingTime> all = new ArrayList<>();

                            ProcessingTime parsed(String interval) {
                                return ProcessingTime.apply(interval);
                            }

                            ProcessingTime created(String interval) {
                                return ProcessingTime.create(interval);
                            }

                            ProcessingTime fromDuration(Duration d) {
                                return ProcessingTime.apply(d);
                            }

                            void register(ProcessingTime t) {
                                all.add(t);
                            }
                        }
                        """,
                        """
                        import java.util.ArrayList;
                        import java.util.List;
                        import org.apache.spark.sql.streaming.Trigger;
                        import scala.concurrent.duration.Duration;

                        class Streams {
                            private final Trigger fixed = Trigger.ProcessingTime(5000);
                            private final List<Trigger> all = new ArrayList<>();

                            Trigger parsed(String interval) {
                                return Trigger.ProcessingTime(interval);
                            }

                            Trigger created(String interval) {
                                return Trigger.ProcessingTime(interval);
                            }

                            Trigger fromDuration(Duration d) {
                                return Trigger.ProcessingTime(d);
                            }

                            void register(Trigger t) {
                                all.add(t);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void rewritesConstructorInAFileThatAlreadyImportsTrigger() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.streaming.ProcessingTime;
                        import org.apache.spark.sql.streaming.Trigger;

                        class Streams {
                            Trigger every5s() {
                                return new ProcessingTime(5000);
                            }

                            Trigger once() {
                                return Trigger.Once();
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.streaming.Trigger;

                        class Streams {
                            Trigger every5s() {
                                return Trigger.ProcessingTime(5000);
                            }

                            Trigger once() {
                                return Trigger.Once();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void keepsTypeInAFileThatTestsForProcessingTime() {
        // As Trigger, the instanceof would also accept Trigger.Once(): the type
        // stays, so the file still fails the 3.5.5 compile gate rather than
        // silently changing behaviour. The factory call is still rewritten.
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.streaming.ProcessingTime;
                        import org.apache.spark.sql.streaming.Trigger;

                        class Streams {
                            Trigger every5s() {
                                return ProcessingTime.apply(5000);
                            }

                            boolean isProcessingTime(Trigger t) {
                                return t instanceof ProcessingTime;
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.streaming.ProcessingTime;
                        import org.apache.spark.sql.streaming.Trigger;

                        class Streams {
                            Trigger every5s() {
                                return Trigger.ProcessingTime(5000);
                            }

                            boolean isProcessingTime(Trigger t) {
                                return t instanceof ProcessingTime;
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
