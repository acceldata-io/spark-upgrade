package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkStoreAssignmentPolicyDetectTest implements RewriteTest {

    private static final String WHY =
            " writes into an existing table's schema. From Spark 3.0 (spark.sql.storeAssignmentPolicy=ANSI) a value " +
                    "that cannot be safely cast to its column's type -- string to int or date, boolean to numeric -- " +
                    "fails the write at analysis, and a numeric overflow fails at run time; 2.4 cast it.";

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkStoreAssignmentPolicyDetect());
    }

    @Test
    void flagsInsertIntoAndAppendingSaveAsTable() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SaveMode;

                        class Writes {
                            void write(Dataset<Row> df, String mode) {
                                df.write().insertInto("billing.usage");
                                df.write().mode(SaveMode.Append).saveAsTable("billing.usage");
                                df.write().mode("append").format("parquet").saveAsTable("billing.usage");
                                df.write().mode(mode).saveAsTable("billing.usage");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SaveMode;

                        class Writes {
                            void write(Dataset<Row> df, String mode) {
                                /*~~(insertInto%1$s)~~>*/df.write().insertInto("billing.usage");
                                /*~~(saveAsTable in append mode%1$s)~~>*/df.write().mode(SaveMode.Append).saveAsTable("billing.usage");
                                /*~~(saveAsTable in append mode%1$s)~~>*/df.write().mode("append").format("parquet").saveAsTable("billing.usage");
                                /*~~(saveAsTable with a mode that may be append%1$s)~~>*/df.write().mode(mode).saveAsTable("billing.usage");
                            }
                        }
                        """.formatted(WHY)
                )
        );
    }

    @Test
    void leavesWritesThatTakeTheirSchemaFromTheDataFrame() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SaveMode;

                        class Writes {
                            void write(Dataset<Row> df) {
                                df.write().saveAsTable("billing.fresh");
                                df.write().mode(SaveMode.Overwrite).saveAsTable("billing.usage");
                                df.write().mode("overwrite").saveAsTable("billing.usage");
                            }
                        }
                        """
                )
        );
    }
}
