package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkIsRunningLocallyDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkIsRunningLocallyDetect());
    }

    @Test
    void flagsCallsTheRewriteLeaves() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.TaskContext;

                        class Tasks {
                            boolean local() {
                                return context().isRunningLocally();
                            }

                            void poll(TaskContext tc) {
                                while (!tc.isRunningLocally()) {
                                    tc.attemptNumber();
                                }
                            }

                            TaskContext context() {
                                return TaskContext.get();
                            }
                        }
                        """,
                        """
                        import org.apache.spark.TaskContext;

                        class Tasks {
                            boolean local() {
                                return /*~~(TaskContext.isRunningLocally() was removed in Spark 3.0; in 2.4 it always returned false. Replace the call with false (the tool did not, because of its receiver or position).)~~>*/context().isRunningLocally();
                            }

                            void poll(TaskContext tc) {
                                while (!/*~~(TaskContext.isRunningLocally() was removed in Spark 3.0; in 2.4 it always returned false. Replace the call with false (the tool did not, because of its receiver or position).)~~>*/tc.isRunningLocally()) {
                                    tc.attemptNumber();
                                }
                            }

                            TaskContext context() {
                                return TaskContext.get();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesCallsTheRewriteCoversAndOtherMethods() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.TaskContext;

                        class Tasks {
                            boolean local(TaskContext tc) {
                                return tc.isRunningLocally() || TaskContext.get().isRunningLocally();
                            }

                            int attempt(TaskContext tc) {
                                return tc.attemptNumber();
                            }
                        }
                        """
                )
        );
    }
}
