package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkUnionAllRenameTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkUnionAllRename());
    }

    @Test
    void rewritesUnionAllToUnion() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Combine {
                            Dataset<Row> combine(Dataset<Row> a, Dataset<Row> b) {
                                return a.unionAll(b);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Combine {
                            Dataset<Row> combine(Dataset<Row> a, Dataset<Row> b) {
                                return a.union(b);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAlreadyCorrectUnionUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Combine {
                            Dataset<Row> combine(Dataset<Row> a, Dataset<Row> b) {
                                return a.union(b);
                            }
                        }
                        """
                )
        );
    }
}
