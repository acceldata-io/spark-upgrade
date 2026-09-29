package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkLog4j1UsageDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkLog4j1UsageDetect());
    }

    @Test
    void flagsLog4j1Import() {
        rewriteRun(
                java(
                        """
                        import org.apache.log4j.Logger;

                        class Job {
                            private static final Logger LOG = Logger.getLogger(Job.class);
                        }
                        """,
                        """
                        /*~~(Log4j 1.x reached end-of-life and Spark itself moved to Log4j 2.x in Spark 3.3; this application's own logging code needs its own migration.)~~>*/import org.apache.log4j.Logger;

                        class Job {
                            private static final Logger LOG = Logger.getLogger(Job.class);
                        }
                        """
                )
        );
    }

    @Test
    void leavesSlf4jImportUntouched() {
        rewriteRun(
                java(
                        """
                        class Job {
                        }
                        """
                )
        );
    }
}
