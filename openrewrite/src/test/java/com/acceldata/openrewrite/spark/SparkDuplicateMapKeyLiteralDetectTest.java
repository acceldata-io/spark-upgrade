package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkDuplicateMapKeyLiteralDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkDuplicateMapKeyLiteralDetect());
    }

    @Test
    void flagsDuplicateLiteralKeys() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.lit;
                        import static org.apache.spark.sql.functions.map;

                        class Lookups {
                            Column build() {
                                return map(lit("a"), lit(1), lit("a"), lit(2));
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.lit;
                        import static org.apache.spark.sql.functions.map;

                        class Lookups {
                            Column build() {
                                return /*~~(This map(...) literal has two equal keys (a); Spark 3.0+ throws a RuntimeException for a duplicate map key instead of silently keeping one value.)~~>*/map(lit("a"), lit(1), lit("a"), lit(2));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesDistinctKeysUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.lit;
                        import static org.apache.spark.sql.functions.map;

                        class Lookups {
                            Column build() {
                                return map(lit("a"), lit(1), lit("b"), lit(2));
                            }
                        }
                        """
                )
        );
    }
}
