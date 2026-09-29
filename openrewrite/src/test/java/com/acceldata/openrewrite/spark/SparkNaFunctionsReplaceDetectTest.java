package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkNaFunctionsReplaceDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkNaFunctionsReplaceDetect());
    }

    @Test
    void flagsNaReplace() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import java.util.Collections;

                        class Cleaner {
                            Dataset<Row> clean(Dataset<Row> df) {
                                return df.na().replace("nested.col", Collections.singletonMap("NA", "unknown"));
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import java.util.Collections;

                        class Cleaner {
                            Dataset<Row> clean(Dataset<Row> df) {
                                return /*~~(na().replace(...)'s column-name matching is stricter from Spark 3.2 onward -- review the column name argument for dots/backticks.)~~>*/df.na().replace("nested.col", Collections.singletonMap("NA", "unknown"));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesUnrelatedDropUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Cleaner {
                            Dataset<Row> clean(Dataset<Row> df) {
                                return df.na().drop();
                            }
                        }
                        """
                )
        );
    }
}
