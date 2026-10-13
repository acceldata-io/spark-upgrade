package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkCalendarIntervalConstructorRewriteTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkCalendarIntervalConstructorRewrite());
    }

    @Test
    void addsZeroDays() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.unsafe.types.CalendarInterval;
                        import static org.apache.spark.sql.functions.lit;

                        class Windows {
                            Column grace(int months, long micros) {
                                return lit(new CalendarInterval(months, micros + 3600000000L)).as("grace");
                            }

                            Object qualified() {
                                return new org.apache.spark.unsafe.types.CalendarInterval(1, 0L);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.unsafe.types.CalendarInterval;
                        import static org.apache.spark.sql.functions.lit;

                        class Windows {
                            Column grace(int months, long micros) {
                                return lit(new CalendarInterval(months, 0, micros + 3600000000L)).as("grace");
                            }

                            Object qualified() {
                                return new org.apache.spark.unsafe.types.CalendarInterval(1, 0, 0L);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesFieldReadsAndLiteralsThatKeepTheirName() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.unsafe.types.CalendarInterval;
                        import static org.apache.spark.sql.functions.lit;

                        class Windows {
                            long micros(CalendarInterval i) {
                                return i.microseconds;
                            }

                            Dataset<Row> project(Dataset<Row> df) {
                                return df.select(lit(new CalendarInterval(1, 0L)));
                            }
                        }
                        """
                )
        );
    }
}
