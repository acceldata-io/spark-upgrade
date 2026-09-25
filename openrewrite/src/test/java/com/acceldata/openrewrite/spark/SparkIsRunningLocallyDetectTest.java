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
    void flagsIsRunningLocally() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.TaskContext;

                        class Tasks {
                            boolean local(TaskContext tc) {
                                return tc.isRunningLocally();
                            }
                        }
                        """,
                        """
                        import org.apache.spark.TaskContext;

                        class Tasks {
                            boolean local(TaskContext tc) {
                                return /*~~(TaskContext.isRunningLocally() was removed in Spark 3.0 along with local execution; the code path it guards is dead.)~~>*/tc.isRunningLocally();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesOtherTaskContextMethodUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.TaskContext;

                        class Tasks {
                            int attempt(TaskContext tc) {
                                return tc.attemptNumber();
                            }
                        }
                        """
                )
        );
    }
}
