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
                            /*~~(CalendarInterval is referenced here. From Spark 3.2, subtracting two dates or two timestamps produces DayTimeIntervalType, not CalendarIntervalType -- so code that stores, casts or checks the result as CalendarInterval no longer type-checks.)~~>*/CalendarInterval interval;
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
                                return lit(new org.apache.spark.unsafe.types./*~~(CalendarInterval is referenced here. From Spark 3.2, subtracting two dates or two timestamps produces DayTimeIntervalType, not CalendarIntervalType -- so code that stores, casts or checks the result as CalendarInterval no longer type-checks.)~~>*/CalendarInterval(0, 3600000000L));
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
