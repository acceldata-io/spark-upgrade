package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkPathOptionConflictRewriteTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkPathOptionConflictRewrite());
    }

    @Test
    void dropsPathOptionBeforeSingleArgumentLoadAndSave() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Ingest {
                            static final String STAGING = "/data/staging";

                            Dataset<Row> read(SparkSession spark, String in) {
                                return spark.read()
                                        .format("parquet")
                                        .option("path", STAGING)
                                        .option("mergeSchema", "true")
                                        .load(in);
                            }

                            void write(Dataset<Row> df, String out) {
                                df.write().option("path", out).mode("overwrite").save("/data/out");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Ingest {
                            static final String STAGING = "/data/staging";

                            Dataset<Row> read(SparkSession spark, String in) {
                                return spark.read()
                                        .format("parquet")
                                        .option("mergeSchema", "true")
                                        .load(in);
                            }

                            void write(Dataset<Row> df, String out) {
                                df.write().mode("overwrite").save("/data/out");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void dropsPathOptionThatStartsTheChain() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.DataFrameReader;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Ingest {
                            Dataset<Row> read(DataFrameReader reader) {
                                return reader.option("path", "/a").load("/b");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.DataFrameReader;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        class Ingest {
                            Dataset<Row> read(DataFrameReader reader) {
                                return reader.load("/b");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesMultiPathLoadAndSideEffectingValuesAndOtherOptions() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Ingest {
                            Dataset<Row> multi(SparkSession spark) {
                                return spark.read().option("path", "/a").load("/b", "/c");
                            }

                            Dataset<Row> computed(SparkSession spark) {
                                return spark.read().option("path", nextPath()).load("/b");
                            }

                            Dataset<Row> noConflict(SparkSession spark) {
                                return spark.read().option("header", "true").load("/b");
                            }

                            String nextPath() {
                                return "/a";
                            }
                        }
                        """
                )
        );
    }
}
