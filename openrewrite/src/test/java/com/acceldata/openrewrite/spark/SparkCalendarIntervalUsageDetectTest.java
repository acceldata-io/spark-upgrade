package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkCalendarIntervalUsageDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkCalendarIntervalUsageDetect());
    }

    @Test
    void flagsCalendarIntervalTypeReference() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.unsafe.types.CalendarInterval;

                        class Intervals {
                            CalendarInterval interval;
                        }
                        """,
                        """
                        import org.apache.spark.unsafe.types.CalendarInterval;

                        class Intervals {
                            /*~~(CalendarInterval is referenced here. Spark 3.0 added a days field and removed the (months, microseconds) constructor, the MICROS_PER_* constants, the from*String parsers, milliseconds(), add, subtract and negate: code calling those no longer compiles, and code reading microseconds silently gets different numbers.)~~>*/CalendarInterval interval;
                        }
                        """
                )
        );
    }

    @Test
    void leavesTheConstructorTheRewriteCovers() {
        rewriteRun(
                java(
                        """
                        class Intervals {
                            Object grace() {
                                return new org.apache.spark.unsafe.types.CalendarInterval(0, 3600000000L);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void flagsAConstructorTheRewriteLeaves() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.lit;

                        class Intervals {
                            Column grace() {
                                return lit(new org.apache.spark.unsafe.types.CalendarInterval(0, 3600000000L));
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.lit;

                        class Intervals {
                            Column grace() {
                                return lit(new org.apache.spark.unsafe.types./*~~(CalendarInterval is referenced here. Spark 3.0 added a days field and removed the (months, microseconds) constructor, the MICROS_PER_* constants, the from*String parsers, milliseconds(), add, subtract and negate: code calling those no longer compiles, and code reading microseconds silently gets different numbers.)~~>*/CalendarInterval(0, 3600000000L));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesUnrelatedFieldUntouched() {
        rewriteRun(
                java(
                        """
                        class Intervals {
                            String interval;
                        }
                        """
                )
        );
    }
}
