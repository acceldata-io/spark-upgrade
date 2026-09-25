package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkHiveContextConstructorDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkHiveContextConstructorDetect());
    }

    @Test
    void flagsDirectConstructionFromSparkContext() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.SparkContext;
                        import org.apache.spark.sql.hive.HiveContext;

                        class Setup {
                            HiveContext build(SparkContext sc) {
                                return new HiveContext(sc);
                            }
                        }
                        """,
                        """
                        import org.apache.spark.SparkContext;
                        import org.apache.spark.sql.hive.HiveContext;

                        class Setup {
                            HiveContext build(SparkContext sc) {
                                return /*~~(HiveContext is removed in Spark 3.x; migrate to SparkSession.builder().enableHiveSupport().getOrCreate().)~~>*/new HiveContext(sc);
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
                                return SparkSession.builder().enableHiveSupport().getOrCreate();
                            }
                        }
                        """
                )
        );
    }
}
