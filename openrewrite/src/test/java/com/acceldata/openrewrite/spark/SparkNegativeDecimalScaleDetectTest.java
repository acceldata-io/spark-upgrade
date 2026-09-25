package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkNegativeDecimalScaleDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkNegativeDecimalScaleDetect());
    }

    @Test
    void flagsNegativeLiteralScale() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.types.DecimalType;

                        class Schemas {
                            DecimalType wide() {
                                return new DecimalType(38, -2);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.types.DecimalType;

                        class Schemas {
                            DecimalType wide() {
                                return /*~~(DecimalType(..., -2) uses a negative scale, which Spark 3.0 rejects at analysis time by default.)~~>*/new DecimalType(38, -2);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesPositiveScaleUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.types.DecimalType;

                        class Schemas {
                            DecimalType normal() {
                                return new DecimalType(38, 2);
                            }
                        }
                        """
                )
        );
    }
}
