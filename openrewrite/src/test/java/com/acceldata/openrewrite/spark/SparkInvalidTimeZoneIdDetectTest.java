package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkInvalidTimeZoneIdDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkInvalidTimeZoneIdDetect());
    }

    @Test
    void flagsUnresolvableTimeZoneId() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;

                        import static org.apache.spark.sql.functions.from_utc_timestamp;

                        class Times {
                            Column local(Column ts) {
                                return from_utc_timestamp(ts, "Not/AZone");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;

                        import static org.apache.spark.sql.functions.from_utc_timestamp;

                        class Times {
                            Column local(Column ts) {
                                return /*~~(Timezone ID "Not/AZone" (passed to a *_utc_timestamp function) is not resolvable by java.time (checked with ZoneId.of(id, ZoneId.SHORT_IDS), the same resolution Spark performs). Spark 2.4 silently fell back to GMT for an unrecognised ID; Spark 3.0 and later throw instead.)~~>*/from_utc_timestamp(ts, "Not/AZone");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesCommonThreeLetterAbbreviationUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;

                        import static org.apache.spark.sql.functions.from_utc_timestamp;

                        class Times {
                            Column local(Column ts) {
                                return from_utc_timestamp(ts, "EST");
                            }
                        }
                        """
                )
        );
    }
}
