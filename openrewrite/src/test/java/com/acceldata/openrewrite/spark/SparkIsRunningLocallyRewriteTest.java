package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkIsRunningLocallyRewriteTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkIsRunningLocallyRewrite());
    }

    @Test
    void replacesCallOnTaskContextGetOrVariableWithFalse() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.TaskContext;

                        class Tasks {
                            String mode() {
                                if (TaskContext.get().isRunningLocally()) {
                                    return "local";
                                }
                                return "cluster";
                            }

                            boolean local(TaskContext tc) {
                                return !tc.isRunningLocally();
                            }
                        }
                        """,
                        """
                        import org.apache.spark.TaskContext;

                        class Tasks {
                            String mode() {
                                if (false) {
                                    return "local";
                                }
                                return "cluster";
                            }

                            boolean local(TaskContext tc) {
                                return !false;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void removesTaskContextImportWhenNoLongerUsed() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.TaskContext;

                        class Tasks {
                            boolean local() {
                                return TaskContext.get().isRunningLocally();
                            }
                        }
                        """,
                        """
                        class Tasks {
                            boolean local() {
                                return false;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesSideEffectingReceiverAndLoopCondition() {
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
                        """
                )
        );
    }
}
