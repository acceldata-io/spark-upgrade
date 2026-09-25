package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkSqlContextConstructorDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkSqlContextConstructorDetect());
    }

    @Test
    void flagsDirectConstructionFromSparkContext() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.SparkContext;
                        import org.apache.spark.sql.SQLContext;

                        class Setup {
                            SQLContext build(SparkContext sc) {
                                return new SQLContext(sc);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.SparkContext;
                        import org.apache.spark.sql.SQLContext;

                        class Setup {
                            SQLContext build(SparkContext sc) {
                                return /*~~(Direct SQLContext construction is deprecated; migrate to SparkSession.builder()...getOrCreate().sqlContext().)~~>*/new SQLContext(sc);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesSparkSessionBuilderUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.SparkSession;

                        class Setup {
                            SparkSession build() {
                                return SparkSession.builder().getOrCreate();
                            }
                        }
                        """
                )
        );
    }
}
