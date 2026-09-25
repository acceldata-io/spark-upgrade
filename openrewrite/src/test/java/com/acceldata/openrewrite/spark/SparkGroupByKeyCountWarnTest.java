package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkGroupByKeyCountWarnTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkGroupByKeyCountWarn());
    }

    @Test
    void flagsGroupByKey() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Encoders;
                        import org.apache.spark.sql.KeyValueGroupedDataset;
                        import org.apache.spark.sql.Row;

                        class Grouping {
                            KeyValueGroupedDataset<String, Row> group(Dataset<Row> ds) {
                                return ds.groupByKey(r -> r.getString(0), Encoders.STRING());
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Encoders;
                        import org.apache.spark.sql.KeyValueGroupedDataset;
                        import org.apache.spark.sql.Row;

                        class Grouping {
                            KeyValueGroupedDataset<String, Row> group(Dataset<Row> ds) {
                                return /*~~(groupByKey's grouping attribute is named "key" from Spark 3.0 onward, not 2.4's "value", for a non-struct key. Review every "value" column reference derived from this pipeline.)~~>*/ds.groupByKey(r -> r.getString(0), Encoders.STRING());
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesUnrelatedMethodUntouched() {
        rewriteRun(
                java(
                        """
                        class Grouping {
                            String groupByKey(String key) {
                                return key;
                            }

                            String use() {
                                return groupByKey("k");
                            }
                        }
                        """
                )
        );
    }
}
