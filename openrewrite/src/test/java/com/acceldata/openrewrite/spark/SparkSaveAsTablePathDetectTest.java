package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkSaveAsTablePathDetectTest implements RewriteTest {

    private static final String WHY =
            ". When the table does not exist but the directory holds files -- the usual rerun, " +
                    "since dropping an external table keeps its files -- Spark 3.2+ fails the write " +
                    "(non-empty CTAS location), where 2.4 overwrote the directory. The tool injects " +
                    "spark.sql.legacy.allowNonEmptyLocationInCTAS=true to keep 2.4's behaviour.";

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkSaveAsTablePathDetect());
    }

    @Test
    void flagsAPathOptionInErrorIfExistsMode() {
        rewriteRun(
                java(
                        """
                        import java.util.Collections;
                        import java.util.Map;

                        import org.apache.spark.sql.DataFrameWriter;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SaveMode;

                        class Writes {
                            void write(Dataset<Row> df, String outPath, String mode) {
                                df.write().option("path", outPath).saveAsTable("sales.daily");
                                df.write().format("parquet").mode(SaveMode.ErrorIfExists).option("path", outPath).saveAsTable("sales.daily");
                                df.write().mode("error").options(Collections.singletonMap("path", outPath)).saveAsTable("sales.daily");
                                df.write().mode(mode).option("path", outPath).saveAsTable("sales.daily");
                                DataFrameWriter<Row> writer = df.write().partitionBy("dt").option("path", outPath);
                                writer.saveAsTable("sales.daily");
                                Map<String, String> opts = Collections.singletonMap("path", outPath);
                                df.write().options(opts).saveAsTable("sales.daily");
                            }
                        }
                        """,
                        """
                        import java.util.Collections;
                        import java.util.Map;

                        import org.apache.spark.sql.DataFrameWriter;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SaveMode;

                        class Writes {
                            void write(Dataset<Row> df, String outPath, String mode) {
                                /*~~(saveAsTable with a `path` option, in ErrorIfExists mode (the default)%1$s)~~>*/df.write().option("path", outPath).saveAsTable("sales.daily");
                                /*~~(saveAsTable with a `path` option, in ErrorIfExists mode (the default)%1$s)~~>*/df.write().format("parquet").mode(SaveMode.ErrorIfExists).option("path", outPath).saveAsTable("sales.daily");
                                /*~~(saveAsTable with a `path` option, in ErrorIfExists mode (the default)%1$s)~~>*/df.write().mode("error").options(Collections.singletonMap("path", outPath)).saveAsTable("sales.daily");
                                /*~~(saveAsTable with a `path` option, in a mode that may be ErrorIfExists%1$s)~~>*/df.write().mode(mode).option("path", outPath).saveAsTable("sales.daily");
                                DataFrameWriter<Row> writer = df.write().partitionBy("dt").option("path", outPath);
                                /*~~(saveAsTable with a `path` option, in ErrorIfExists mode (the default)%1$s)~~>*/writer.saveAsTable("sales.daily");
                                Map<String, String> opts = Collections.singletonMap("path", outPath);
                                /*~~(saveAsTable with a `path` option, in ErrorIfExists mode (the default)%1$s)~~>*/df.write().options(opts).saveAsTable("sales.daily");
                            }
                        }
                        """.formatted(WHY)
                )
        );
    }

    @Test
    void leavesWritesTheCheckNeverReaches() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SaveMode;

                        class Writes {
                            void write(Dataset<Row> df, String outPath, org.apache.spark.sql.SparkSession spark) {
                                df.write().saveAsTable("sales.managed");
                                df.write().mode(SaveMode.Overwrite).option("path", outPath).saveAsTable("sales.daily");
                                df.write().mode("append").option("path", outPath).saveAsTable("sales.daily");
                                df.write().mode(SaveMode.Ignore).option("path", outPath).saveAsTable("sales.daily");
                                df.write().format("hive").option("path", outPath).saveAsTable("sales.daily");
                                Dataset<Row> raw = spark.read().option("path", "/landing/sales").format("parquet").load();
                                raw.write().saveAsTable("sales.raw");
                            }
                        }
                        """
                )
        );
    }
}
