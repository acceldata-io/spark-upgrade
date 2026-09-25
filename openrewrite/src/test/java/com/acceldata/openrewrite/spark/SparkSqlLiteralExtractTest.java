package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkSqlLiteralExtractTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkSqlLiteralExtract());
    }

    @Test
    void marksLiteralSqlPassedToSparkSessionSql() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Queries {
                            Dataset<Row> run(SparkSession spark) {
                                return spark.sql("SELECT * FROM t");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Queries {
                            Dataset<Row> run(SparkSession spark) {
                                return spark.sql(/*~~(SELECT * FROM t)~~>*/"SELECT * FROM t");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesNonLiteralSqlUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Queries {
                            Dataset<Row> run(SparkSession spark, String table) {
                                return spark.sql("SELECT * FROM " + table);
                            }
                        }
                        """
                )
        );
    }
}
