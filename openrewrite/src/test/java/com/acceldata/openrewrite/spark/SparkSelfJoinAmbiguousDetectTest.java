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

    private static final String FOUND = "/*~~(df.join(df, ...) joins a DataFrame to itself using the exact same reference " +
            "on both sides, and a column is referenced through that frame (in the condition or after the join); Spark " +
            "3.0's analyzer fails this as an ambiguous self-join where 2.4 silently resolved it to one side.)~~>*/";

    @Test
    void flagsAColumnReferencedThroughTheFrame() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Joins {
                            Dataset<Row> condition(Dataset<Row> df) {
                                return df.join(df, df.col("n").gt(df.col("n")));
                            }

                            Dataset<Row> otherName(Dataset<Row> df) {
                                return df.join(df, df.col("a").equalTo(df.col("b")));
                            }

                            Dataset<Row> after(Dataset<Row> df) {
                                return df.join(df, "id").select(df.col("v"));
                            }

                            Dataset<Row> later(Dataset<Row> df) {
                                Dataset<Row> joined = df.join(df, "id");
                                return joined.filter(df.apply("n").gt(15));
                            }

                            Dataset<Row> opaque(Dataset<Row> df, org.apache.spark.sql.Column cond) {
                                return df.join(df, cond);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Joins {
                            Dataset<Row> condition(Dataset<Row> df) {
                                return %1$sdf.join(df, df.col("n").gt(df.col("n")));
                            }

                            Dataset<Row> otherName(Dataset<Row> df) {
                                return %1$sdf.join(df, df.col("a").equalTo(df.col("b")));
                            }

                            Dataset<Row> after(Dataset<Row> df) {
                                return %1$sdf.join(df, "id").select(df.col("v"));
                            }

                            Dataset<Row> later(Dataset<Row> df) {
                                Dataset<Row> joined = %1$sdf.join(df, "id");
                                return joined.filter(df.apply("n").gt(15));
                            }

                            Dataset<Row> opaque(Dataset<Row> df, org.apache.spark.sql.Column cond) {
                                return %1$sdf.join(df, cond);
                            }
                        }
                        """.formatted(FOUND)
                )
        );
    }

    @Test
    void leavesTheShapes355ResolvesUntouched() {
        // Probed: the same rows and columns on 2.4.8 and 3.5.5.
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Joins {
                            Dataset<Row> byName(Dataset<Row> df) {
                                return df.join(df, "id");
                            }

                            Dataset<Row> sameName(Dataset<Row> df) {
                                return df.join(df, df.col("id").equalTo(df.col("id")).and(df.col("v").equalTo(df.col("v"))));
                            }

                            Dataset<Row> dropped(Dataset<Row> df) {
                                return df.join(df, "id").drop(df.col("v"));
                            }

                            Dataset<Row> twoFrames(Dataset<Row> a, Dataset<Row> b) {
                                return a.join(b, a.col("n").gt(b.col("n")));
                            }
                        }
                        """
                )
        );
    }
}
