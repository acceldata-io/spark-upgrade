package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkSelfJoinAmbiguousDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkSelfJoinAmbiguousDetect());
    }

    @Test
    void flagsSelfJoinOnSameReference() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Joins {
                            Dataset<Row> selfJoin(Dataset<Row> df) {
                                return df.join(df, "id");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Joins {
                            Dataset<Row> selfJoin(Dataset<Row> df) {
                                return /*~~(df.join(df, ...) joins a DataFrame to itself using the exact same reference on both sides; Spark 3.0's analyzer can fail this as an ambiguous self-join instead of resolving column references as 2.4 did.)~~>*/df.join(df, "id");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesJoinOfTwoDifferentDatasetsUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Joins {
                            Dataset<Row> join(Dataset<Row> a, Dataset<Row> b) {
                                return a.join(b, "id");
                            }
                        }
                        """
                )
        );
    }
}
