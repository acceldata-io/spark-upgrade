package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkCommandOutputSchemaDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkCommandOutputSchemaDetect());
    }

    private static final String TAIL = "Code that reads the result by the 2.4 name fails; positional access is " +
            "unaffected. spark.sql.legacy.keepCommandOutputSchema=true restores 2.4's names.)~~>*/";

    @Test
    void flagsTheRenamedCommands() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.SQLContext;
                        import org.apache.spark.sql.SparkSession;

                        class Catalog {
                            void list(SparkSession spark, SQLContext sqlContext, String db) {
                                spark.sql("SHOW DATABASES").collectAsList();
                                sqlContext.sql("show tables in " + db);
                                spark.sql("DESCRIBE DATABASE EXTENDED sales");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.SQLContext;
                        import org.apache.spark.sql.SparkSession;

                        class Catalog {
                            void list(SparkSession spark, SQLContext sqlContext, String db) {
                                /*~~(SHOW DATABASES: Spark 3.0 renamed its output columns (databaseName -> namespace). %1$sspark.sql("SHOW DATABASES").collectAsList();
                                /*~~(SHOW TABLES: Spark 3.0 renamed its output columns (database -> namespace). %1$ssqlContext.sql("show tables in " + db);
                                /*~~(DESCRIBE DATABASE EXTENDED: Spark 3.0 renamed its output columns (database_description_item/database_description_value -> info_name/info_value). %1$sspark.sql("DESCRIBE DATABASE EXTENDED sales");
                            }
                        }
                        """.formatted(TAIL)
                )
        );
    }

    @Test
    void leavesOtherSqlUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.SparkSession;

                        class Catalog {
                            void list(SparkSession spark, String query) {
                                spark.sql("SHOW PARTITIONS sales.orders");
                                spark.sql("DESCRIBE TABLE sales.orders");
                                spark.sql("SELECT 'SHOW DATABASES' AS s");
                                spark.sql(query);
                            }
                        }
                        """
                )
        );
    }
}
