package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkMesosUsageDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkMesosUsageDetect());
    }

    @Test
    void flagsMesosImport() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.scheduler.cluster.mesos.MesosClusterManager;

                        class Deploy {
                        }
                        """,
                        """
                        /*~~(Mesos is deprecated in Spark 3.2 and removed in Spark 4.0 -- this application integrates with Mesos as a cluster manager.)~~>*/import org.apache.spark.scheduler.cluster.mesos.MesosClusterManager;

                        class Deploy {
                        }
                        """
                )
        );
    }

    @Test
    void leavesUnrelatedImportUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.SparkSession;

                        class Deploy {
                        }
                        """
                )
        );
    }
}
