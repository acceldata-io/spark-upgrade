package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkDateTimeFormatPatternDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkDateTimeFormatPatternDetect());
    }

    @Test
    void flagsToDatePattern() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.to_date;

                        class Dates {
                            Column parse() {
                                return to_date(col("raw"), "yyyy-MM-dd");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.to_date;

                        class Dates {
                            Column parse() {
                                return /*~~(This datetime pattern is parsed strictly from Spark 3.0 onward; review it for compatibility with the old, more lenient parser.)~~>*/to_date(col("raw"), "yyyy-MM-dd");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesNoArgToDateUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.to_date;

                        class Dates {
                            Column parse() {
                                return to_date(col("raw"));
                            }
                        }
                        """
                )
        );
    }
}
