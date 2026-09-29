package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkMapTypeKeyInCreateMapDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkMapTypeKeyInCreateMapDetect());
    }

    @Test
    void flagsNestedMapAsKey() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.lit;
                        import static org.apache.spark.sql.functions.map;

                        class Lookups {
                            Column build() {
                                return map(map(lit("a"), lit(1)), lit("outer"));
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.lit;
                        import static org.apache.spark.sql.functions.map;

                        class Lookups {
                            Column build() {
                                return /*~~(This map(...)'s key is itself a map(...) literal (MapType); Spark 3.0's analyzer rejects a MapType key with an AnalysisException.)~~>*/map(map(lit("a"), lit(1)), lit("outer"));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesScalarKeyUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import static org.apache.spark.sql.functions.lit;
                        import static org.apache.spark.sql.functions.map;

                        class Lookups {
                            Column build() {
                                return map(lit("a"), lit(1));
                            }
                        }
                        """
                )
        );
    }
}
