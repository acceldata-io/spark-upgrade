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
    void flagsAPattern355ReadsDifferently() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.to_date;

                        class Dates {
                            Column parse() {
                                return to_date(col("raw"), "dd/MM/yyyy hh:mm");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.to_date;

                        class Dates {
                            Column parse() {
                                return /*~~(Pattern "dd/MM/yyyy hh:mm" parses the 12-hour clock 'h' with no 'a' (am/pm) marker. 2.4 parsed 00-23 leniently; Spark 3.x returns NULL for any hour outside 1-12. 2.4 used SimpleDateFormat; Spark 3.x uses DateTimeFormatter.)~~>*/to_date(col("raw"), "dd/MM/yyyy hh:mm");
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

    @Test
    void flagsWeekBasedAndFormattingOnlyLetters() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.*;

                        class Dates {
                            void patterns() {
                                Column a = date_format(col("d"), "YYYY-MM-dd");
                                Column b = to_timestamp(col("d"), "EEE, dd MMM yyyy");
                                Column c = from_unixtime(col("t"), "F");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.*;

                        class Dates {
                            void patterns() {
                                Column a = /*~~(Pattern "YYYY-MM-dd" uses 'Y', the week-based year -- almost always meant as `y`, the calendar year. Spark 3.0+ rejects week-based pattern letters outright, so the call fails. 2.4 used SimpleDateFormat; Spark 3.x uses DateTimeFormatter.)~~>*/date_format(col("d"), "YYYY-MM-dd");
                                Column b = /*~~(Pattern "EEE, dd MMM yyyy" uses 'E' (the day-of-week name) to parse. Spark 3.0+ can format with it but refuses to parse with it. 2.4 used SimpleDateFormat; Spark 3.x uses DateTimeFormatter.)~~>*/to_timestamp(col("d"), "EEE, dd MMM yyyy");
                                Column c = /*~~(Pattern "F" uses 'F', which meant week-of-month in 2.4 and means aligned-day-of-week-in-month from 3.0 -- same pattern, different result. 2.4 used SimpleDateFormat; Spark 3.x uses DateTimeFormatter.)~~>*/from_unixtime(col("t"), "F");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesPatterns355ReadsAlikeUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.*;

                        class Dates {
                            void patterns() {
                                Column a = to_date(col("d"), "yyyy-MM-dd");
                                Column b = date_format(col("d"), "EEE, dd MMM yyyy HH:mm:ss");
                                Column c = to_timestamp(col("d"), "yyyy-MM-dd hh:mm a");
                                Column e = to_timestamp(col("d"), "yyyy-MM-dd'T'HH:mm:ss'Z'");
                            }
                        }
                        """
                )
        );
    }
}
