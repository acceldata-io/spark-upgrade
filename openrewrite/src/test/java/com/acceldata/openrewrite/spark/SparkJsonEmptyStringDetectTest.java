package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkJsonEmptyStringDetectTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkJsonEmptyStringDetect());
    }

    @Test
    void flagsJsonReadWithExplicitSchema() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;
                        import org.apache.spark.sql.types.DataTypes;
                        import org.apache.spark.sql.types.StructType;

                        class Reads {
                            Dataset<Row> read(SparkSession spark) {
                                StructType schema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[0]);
                                return spark.read().schema(schema).json("/data/in.json");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;
                        import org.apache.spark.sql.types.DataTypes;
                        import org.apache.spark.sql.types.StructType;

                        class Reads {
                            Dataset<Row> read(SparkSession spark) {
                                StructType schema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[0]);
                                return /*~~(JSON read with an explicit schema. From Spark 3.0 an empty string is no longer accepted as a value for a non-String/non-Binary field -- 2.4 read it as null, 3.0 treats the record as malformed.)~~>*/spark.read().schema(schema).json("/data/in.json");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesInferredSchemaJsonReadUntouched() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;

                        class Reads {
                            Dataset<Row> read(SparkSession spark) {
                                return spark.read().json("/data/in.json");
                            }
                        }
                        """
                )
        );
    }
}
