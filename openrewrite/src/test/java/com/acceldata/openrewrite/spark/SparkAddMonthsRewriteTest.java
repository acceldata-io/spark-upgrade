package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkAddMonthsRewriteTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkAddMonthsRewrite());
    }

    @Test
    void rewritesStaticImportedCallInWithColumn() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import static org.apache.spark.sql.functions.add_months;
                        import static org.apache.spark.sql.functions.col;

                        class Billing {
                            Dataset<Row> nextCycle(Dataset<Row> df) {
                                return df.withColumn("next_billing", add_months(col("billing_date"), 1));
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import static org.apache.spark.sql.functions.add_months;
                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.*;

                        class Billing {
                            Dataset<Row> nextCycle(Dataset<Row> df) {
                                return df.withColumn("next_billing", when(to_date(col("billing_date")).equalTo(last_day(col("billing_date"))), last_day(add_months(col("billing_date"), 1))).otherwise(add_months(col("billing_date"), 1)));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void keepsExistingStaticImportsAndOtherCallsAsTheyAre() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import static org.apache.spark.sql.functions.add_months;
                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.to_date;

                        class Billing {
                            Dataset<Row> nextCycle(Dataset<Row> df) {
                                return df.withColumn("next_billing", add_months(col("billing_date"), 1));
                            }

                            Column parsed() {
                                return to_date(col("raw_date"), "yyyy-MM-dd");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import static org.apache.spark.sql.functions.add_months;
                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.last_day;
                        import static org.apache.spark.sql.functions.to_date;
                        import static org.apache.spark.sql.functions.when;

                        class Billing {
                            Dataset<Row> nextCycle(Dataset<Row> df) {
                                return df.withColumn("next_billing", when(to_date(col("billing_date")).equalTo(last_day(col("billing_date"))), last_day(add_months(col("billing_date"), 1))).otherwise(add_months(col("billing_date"), 1)));
                            }

                            Column parsed() {
                                return to_date(col("raw_date"), "yyyy-MM-dd");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void rewritesQualifiedCallWithAliasAndVariableMonths() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.functions;

                        class Billing {
                            Dataset<Row> shifted(Dataset<Row> df, int months) {
                                return df.select(df.col("id"), functions.add_months(df.col("billing_date"), months).as("shifted"));
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.functions;

                        class Billing {
                            Dataset<Row> shifted(Dataset<Row> df, int months) {
                                return df.select(df.col("id"), functions.when(functions.to_date(df.col("billing_date")).equalTo(functions.last_day(df.col("billing_date"))), functions.last_day(functions.add_months(df.col("billing_date"), months))).otherwise(functions.add_months(df.col("billing_date"), months)).as("shifted"));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void rewritesInsideFilterConditionAndNestedExpression() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import static org.apache.spark.sql.functions.*;

                        class Billing {
                            Dataset<Row> recent(Dataset<Row> df, Column cutoff) {
                                return df.filter(add_months(to_date(col("ts"), "yyyy-MM-dd HH:mm"), -1).lt(cutoff));
                            }

                            Dataset<Row> label(Dataset<Row> df, Column due) {
                                return df.withColumn("label", date_format(add_months(due, 3), "yyyy-MM"));
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import static org.apache.spark.sql.functions.*;

                        class Billing {
                            Dataset<Row> recent(Dataset<Row> df, Column cutoff) {
                                return df.filter(when(to_date(to_date(col("ts"), "yyyy-MM-dd HH:mm")).equalTo(last_day(to_date(col("ts"), "yyyy-MM-dd HH:mm"))), last_day(add_months(to_date(col("ts"), "yyyy-MM-dd HH:mm"), -1))).otherwise(add_months(to_date(col("ts"), "yyyy-MM-dd HH:mm"), -1)).lt(cutoff));
                            }

                            Dataset<Row> label(Dataset<Row> df, Column due) {
                                return df.withColumn("label", date_format(when(to_date(due).equalTo(last_day(due)), last_day(add_months(due, 3))).otherwise(add_months(due, 3)), "yyyy-MM"));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void qualifiesWhenTheFileDeclaresAClashingMethod() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import static org.apache.spark.sql.functions.add_months;
                        import static org.apache.spark.sql.functions.col;

                        class Billing {
                            Dataset<Row> nextCycle(Dataset<Row> df) {
                                return df.withColumn("next_billing", add_months(col("billing_date"), 1));
                            }

                            String when(String event) {
                                return event;
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.functions;

                        import static org.apache.spark.sql.functions.add_months;
                        import static org.apache.spark.sql.functions.col;

                        class Billing {
                            Dataset<Row> nextCycle(Dataset<Row> df) {
                                return df.withColumn("next_billing", functions.when(functions.to_date(col("billing_date")).equalTo(functions.last_day(col("billing_date"))), functions.last_day(functions.add_months(col("billing_date"), 1))).otherwise(functions.add_months(col("billing_date"), 1)));
                            }

                            String when(String event) {
                                return event;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesCallsItCannotRewriteExactly() {
        // A kept auto-generated column name, an argument that is not safe to
        // evaluate twice, and a Column held for later use are left to
        // SparkAddMonthsUsageWarn; last_day(add_months(...)) and the recipe's
        // own output need no change.
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import static org.apache.spark.sql.functions.*;

                        class Billing {
                            Dataset<Row> named(Dataset<Row> df) {
                                return df.select(add_months(col("billing_date"), 1));
                            }

                            Dataset<Row> computed(Dataset<Row> df) {
                                return df.withColumn("next", add_months(expr("billing_date + interval 1 day"), 1))
                                        .withColumn("later", add_months(col("billing_date"), months()));
                            }

                            Column held() {
                                return add_months(col("billing_date"), 1);
                            }

                            Dataset<Row> monthEnd(Dataset<Row> df) {
                                return df.select(last_day(add_months(col("billing_date"), 1)));
                            }

                            Dataset<Row> alreadyRewritten(Dataset<Row> df) {
                                return df.withColumn("next", when(to_date(col("d")).equalTo(last_day(col("d"))), last_day(add_months(col("d"), 1))).otherwise(add_months(col("d"), 1)));
                            }

                            int months() {
                                return 2;
                            }
                        }
                        """
                )
        );
    }
}
