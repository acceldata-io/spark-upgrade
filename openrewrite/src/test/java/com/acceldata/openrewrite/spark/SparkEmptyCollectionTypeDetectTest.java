package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkEmptyCollectionTypeDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkEmptyCollectionTypeDetect());
    }

    @Test
    void flagsNoArgArrayCall() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.functions;

                        class Schemas {
                            Column emptyArray() {
                                return functions.array();
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.functions;

                        class Schemas {
                            Column emptyArray() {
                                return /*~~(array() called with no arguments infers NullType element(s) in Spark 3.0, not 2.4's StringType.)~~>*/functions.array();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void flagsNoArgMapCall() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.functions;

                        class Schemas {
                            Column emptyMap() {
                                return functions.map();
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.functions;

                        class Schemas {
                            Column emptyMap() {
                                return /*~~(map() called with no arguments infers NullType element(s) in Spark 3.0, not 2.4's StringType.)~~>*/functions.map();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesArrayWithArgumentsUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.functions;

                        class Schemas {
                            Column oneElementArray(Column c) {
                                return functions.array(c);
                            }
                        }
                        """
                )
        );
    }
}
