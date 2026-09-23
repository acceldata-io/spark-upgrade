package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkPathOptionConflictDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkPathOptionConflictDetect());
    }

    @Test
    void flagsPathOptionCoexistingWithPathArgumentToLoad() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Ingest {
                            Dataset<Row> read(SparkSession spark) {
                                return spark.read().option("path", "/data/in").load("/data/in");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Ingest {
                            Dataset<Row> read(SparkSession spark) {
                                return /*~~(A `path` option and a path argument to .load(...) coexist on the same chain; Spark 3.1+ rejects this instead of silently choosing one.)~~>*/spark.read().option("path", "/data/in").load("/data/in");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesLoadWithNoPathOptionUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Ingest {
                            Dataset<Row> read(SparkSession spark) {
                                return spark.read().load("/data/in");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesPathOptionWithNoLoadArgumentUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Ingest {
                            Dataset<Row> read(SparkSession spark) {
                                return spark.read().option("path", "/data/in").load();
                            }
                        }
                        """
                )
        );
    }
}
