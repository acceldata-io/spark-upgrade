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
    void marksTemplatesWithAPlaceholderPerRunTimePart() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Queries {
                            static final String ORDERS = "orders";
                            static final String DAILY = "SELECT d, count(*) FROM " + ORDERS + " GROUP BY d";

                            void run(SparkSession spark, String table, String day) {
                                spark.sql("SELECT * FROM " + table + " LIMIT " + 10);
                                spark.sql(String.format("SELECT * FROM %s WHERE d = '%s' AND p LIKE 'a%%' LIMIT %d", table, day, 10));
                                spark.sql(DAILY);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Queries {
                            static final String ORDERS = "orders";
                            static final String DAILY = "SELECT d, count(*) FROM " + ORDERS + " GROUP BY d";

                            void run(SparkSession spark, String table, String day) {
                                spark.sql(/*~~(@@template:1@@SELECT * FROM __param__ LIMIT 10)~~>*/"SELECT * FROM " + table + " LIMIT " + 10);
                                spark.sql(/*~~(@@template:3@@SELECT * FROM __param__ WHERE d = '__param__' AND p LIKE 'a%' LIMIT 0)~~>*/String.format("SELECT * FROM %s WHERE d = '%s' AND p LIKE 'a%%' LIMIT %d", table, day, 10));
                                spark.sql(/*~~(@@template:0@@SELECT d, count(*) FROM orders GROUP BY d)~~>*/DAILY);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void marksSqlBuiltAtRunTimeAsDynamic() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.SparkSession;

                        class Queries {
                            void run(SparkSession spark, String query) {
                                String q = "SELECT 1";
                                q += " FROM t";
                                spark.sql(query);
                                spark.sql(q);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.SparkSession;

                        class Queries {
                            void run(SparkSession spark, String query) {
                                String q = "SELECT 1";
                                q += " FROM t";
                                spark.sql(/*~~(@@dynamic@@)~~>*/query);
                                spark.sql(/*~~(@@dynamic@@)~~>*/q);
                            }
                        }
                        """
                )
        );
    }
}
