package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkHashOnMapTypeDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkHashOnMapTypeDetect());
    }

    @Test
    void flagsHashOfMap() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;

                        import static org.apache.spark.sql.functions.hash;
                        import static org.apache.spark.sql.functions.map;
                        import static org.apache.spark.sql.functions.lit;

                        class Hashing {
                            Column hashed() {
                                return hash(map(lit("k"), lit("v")));
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;

                        import static org.apache.spark.sql.functions.hash;
                        import static org.apache.spark.sql.functions.map;
                        import static org.apache.spark.sql.functions.lit;

                        class Hashing {
                            Column hashed() {
                                return /*~~(hash() applied to a map(...) result; Spark 3.0 throws on MapType input instead of hashing it.)~~>*/hash(map(lit("k"), lit("v")));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesHashOfColumnUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;

                        import static org.apache.spark.sql.functions.col;
                        import static org.apache.spark.sql.functions.hash;

                        class Hashing {
                            Column hashed() {
                                return hash(col("id"));
                            }
                        }
                        """
                )
        );
    }
}
