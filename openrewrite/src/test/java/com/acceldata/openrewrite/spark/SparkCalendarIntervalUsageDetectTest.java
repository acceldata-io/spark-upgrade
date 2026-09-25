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
