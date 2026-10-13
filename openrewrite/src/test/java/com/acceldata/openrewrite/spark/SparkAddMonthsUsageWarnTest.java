package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkAddMonthsUsageWarnTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkAddMonthsUsageWarn());
    }

    @Test
    void flagsAddMonths() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.add_months;
                        import static org.apache.spark.sql.functions.col;

                        class Dates {
                            Column nextBillingCycle() {
                                return add_months(col("billing_date"), 1);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.add_months;
                        import static org.apache.spark.sql.functions.col;

                        class Dates {
                            Column nextBillingCycle() {
                                return /*~~(add_months(...) no longer returns the last day of the month for a month-end input in Spark 3.0. If 2.4's result matters, rewrite it as when(to_date(d).equalTo(last_day(d)), last_day(add_months(d, n))).otherwise(add_months(d, n)), and alias it if its column name matters.)~~>*/add_months(col("billing_date"), 1);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesCallsTheRewriteCoversOrThatNeedNoChange() {
        // SparkAddMonthsRewrite owns the withColumn call; last_day(add_months(...))
        // has the same result in 2.4 and 3.x.
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import static org.apache.spark.sql.functions.*;

                        class Dates {
                            Dataset<Row> nextBillingCycle(Dataset<Row> df) {
                                return df.withColumn("next", add_months(col("billing_date"), 1))
                                        .withColumn("next_month_end", last_day(add_months(col("billing_date"), 1)));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesUnrelatedDateFunctionUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.current_date;

                        class Dates {
                            Column today() {
                                return current_date();
                            }
                        }
                        """
                )
        );
    }
}
