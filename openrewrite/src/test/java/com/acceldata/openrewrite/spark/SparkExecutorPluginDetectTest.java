package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkExecutorPluginDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkExecutorPluginDetect());
    }

    @Test
    void flagsClassImplementingExecutorPlugin() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.ExecutorPlugin;

                        class MyPlugin implements ExecutorPlugin {
                        }
                        """,
                        """
                        import org.apache.spark.ExecutorPlugin;

                        /*~~(ExecutorPlugin was removed in Spark 3.0; migrate to org.apache.spark.api.plugin.SparkPlugin.)~~>*/class MyPlugin implements ExecutorPlugin {
                        }
                        """
                )
        );
    }

    @Test
    void leavesUnrelatedInterfaceUntouched() {
        rewriteRun(
                java(
                        """
                        import java.io.Serializable;

                        class MyPlugin implements Serializable {
                        }
                        """
                )
        );
    }
}
