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
                                return /*~~(add_months(...)'s month-end snapping behavior changed in Spark 3.0 -- review whether the input date can land on the last day of a month.)~~>*/add_months(col("billing_date"), 1);
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
