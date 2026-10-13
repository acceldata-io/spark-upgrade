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
    void flagsCountAggAndReduceGroups() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.api.java.function.MapFunction;
                        import org.apache.spark.api.java.function.ReduceFunction;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Encoders;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.expressions.javalang.typed;
                        import scala.Tuple2;

                        class Grouping {
                            Dataset<Tuple2<String, Object>> counts(Dataset<Row> ds) {
                                return ds.groupByKey((MapFunction<Row, String>) r -> r.getString(0), Encoders.STRING()).count();
                            }

                            Dataset<Tuple2<String, Long>> sums(Dataset<Row> ds) {
                                return ds.groupByKey((MapFunction<Row, String>) r -> r.getString(0), Encoders.STRING())
                                        .agg(typed.sumLong((MapFunction<Row, Long>) r -> r.getLong(1)));
                            }

                            Dataset<Tuple2<Integer, String>> longest(Dataset<String> ds) {
                                return ds.groupByKey((MapFunction<String, Integer>) String::length, Encoders.INT())
                                        .reduceGroups((ReduceFunction<String>) (a, b) -> a.length() >= b.length() ? a : b);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.api.java.function.MapFunction;
                        import org.apache.spark.api.java.function.ReduceFunction;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Encoders;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.expressions.javalang.typed;
                        import scala.Tuple2;

                        class Grouping {
                            Dataset<Tuple2<String, Object>> counts(Dataset<Row> ds) {
                                return /*~~(For a non-struct key, this result's key column is named "key" from Spark 3.0, not 2.4's "value"; spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue=true restores "value".)~~>*/ds.groupByKey((MapFunction<Row, String>) r -> r.getString(0), Encoders.STRING()).count();
                            }

                            Dataset<Tuple2<String, Long>> sums(Dataset<Row> ds) {
                                return /*~~(For a non-struct key, this result's key column is named "key" from Spark 3.0, not 2.4's "value"; spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue=true restores "value".)~~>*/ds.groupByKey((MapFunction<Row, String>) r -> r.getString(0), Encoders.STRING())
                                        .agg(typed.sumLong((MapFunction<Row, Long>) r -> r.getLong(1)));
                            }

                            Dataset<Tuple2<Integer, String>> longest(Dataset<String> ds) {
                                return /*~~(For a non-struct key, this result's key column is named "key" from Spark 3.0, not 2.4's "value"; spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue=true restores "value".)~~>*/ds.groupByKey((MapFunction<String, Integer>) String::length, Encoders.INT())
                                        .reduceGroups((ReduceFunction<String>) (a, b) -> a.length() >= b.length() ? a : b);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesUnaffectedGroupedOperationsUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.api.java.function.MapFunction;
                        import org.apache.spark.api.java.function.MapGroupsFunction;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Encoders;
                        import org.apache.spark.sql.KeyValueGroupedDataset;

                        class Grouping {
                            void use(Dataset<String> ds) {
                                KeyValueGroupedDataset<Integer, String> byLength =
                                        ds.groupByKey((MapFunction<String, Integer>) String::length, Encoders.INT());
                                Dataset<Integer> sizes = byLength.mapGroups(
                                        (MapGroupsFunction<Integer, String, Integer>) (k, it) -> k, Encoders.INT());
                                Dataset<Integer> keys = byLength.keys();
                                long rows = ds.count();
                            }
                        }
                        """
                )
        );
    }
}
