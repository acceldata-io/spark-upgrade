package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkDateTimeStringComparisonDetectTest implements RewriteTest {

    private static final String WHY =
            "A column is compared with a date-shaped string in a way whose result can change. If the " +
                    "column is a date or timestamp, Spark 3.0 casts the string to date/timestamp, where " +
                    "2.4 cast the column to string and compared text: ts > '2020-01-01' kept midnight " +
                    "rows on 2.4 and drops them on 3.x, and d = '2020-1-1' matched nothing on 2.4. The " +
                    "tool injects spark.sql.legacy.typeCoercion.datetimeToString.enabled=true to keep " +
                    "2.4's behaviour; its cost is that a date partition column compared to a string is " +
                    "no longer pruned.";

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkDateTimeStringComparisonDetect());
    }

    @Test
    void flagsComparisonsWithADateShapedString() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.lit;

                        class Filters {
                            private static final String CUTOFF = "2020-01-01";

                            void filter(Dataset<Row> events) {
                                Column start = lit("2019-12-01 00:00:00");
                                events.filter(col("start_date").geq("2017-7-1"));
                                events.filter(col("event_date").equalTo(lit("2020-1-1")));
                                events.filter(col("event_ts").geq(start));
                                events.filter(col("event_ts").gt("2020-01-01"));
                                events.filter(col("event_ts").between("2020-01-01", "2020-01-31"));
                                events.filter(lit("2020-01-01").geq(col("event_ts")));
                                events.filter(col("event_ts").leq(CUTOFF));
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.lit;

                        class Filters {
                            private static final String CUTOFF = "2020-01-01";

                            void filter(Dataset<Row> events) {
                                Column start = lit("2019-12-01 00:00:00");
                                events.filter(/*~~(%1$s)~~>*/col("start_date").geq("2017-7-1"));
                                events.filter(/*~~(%1$s)~~>*/col("event_date").equalTo(lit("2020-1-1")));
                                events.filter(/*~~(%1$s)~~>*/col("event_ts").geq(start));
                                events.filter(/*~~(%1$s)~~>*/col("event_ts").gt("2020-01-01"));
                                events.filter(/*~~(%1$s)~~>*/col("event_ts").between("2020-01-01", "2020-01-31"));
                                events.filter(/*~~(%1$s)~~>*/lit("2020-01-01").geq(col("event_ts")));
                                events.filter(/*~~(%1$s)~~>*/col("event_ts").leq(CUTOFF));
                            }
                        }
                        """.formatted(WHY)
                )
        );
    }

    @Test
    void leavesOtherComparisonsUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;

                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.lit;
                        import static org.apache.spark.sql.functions.to_date;

                        class Filters {
                            void filter(Dataset<Row> events, String day) {
                                events.filter(col("status").equalTo("ACTIVE"));
                                events.filter(col("event_date").equalTo("2020-01-01"));
                                events.filter(col("event_date").geq("2020-01-01"));
                                events.filter(col("event_date").lt(lit("2020-02-01")));
                                events.filter(lit("2020-01-01").leq(col("event_ts")));
                                events.filter(col("event_date").geq(to_date(lit("2020-01-01"))));
                                events.filter(col("event_date").geq(day));
                                boolean same = "2020-01-01".equals(day);
                            }
                        }
                        """
                )
        );
    }
}
