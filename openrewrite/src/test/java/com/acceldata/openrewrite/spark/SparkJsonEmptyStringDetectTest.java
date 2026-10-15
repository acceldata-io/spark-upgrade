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

    private static final String WHY = "JSON read with an explicit schema: from Spark 3.0 an empty string in a non-string " +
            "field makes the record malformed, which here ";

    @Test
    void flagsTheModesThatShowAMalformedRecord() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;
                        import org.apache.spark.sql.types.StructType;

                        class Reads {
                            void read(SparkSession spark, StructType schema, String mode, java.util.Map<String, String> opts) {
                                Dataset<Row> a = spark.read().option("mode", "FAILFAST").schema(schema).json("/data/in.json");
                                Dataset<Row> b = spark.read().schema(schema).option("mode", "dropMalformed").json("/data/in.json");
                                Dataset<Row> c = spark.read().schema("n INT, _corrupt_record STRING").json("/data/in.json");
                                Dataset<Row> d = spark.read().option("columnNameOfCorruptRecord", "bad").schema(schema).json("/data/in.json");
                                Dataset<Row> e = spark.read().option("mode", mode).schema(schema).json("/data/in.json");
                                Dataset<Row> f = spark.read().options(opts).schema(schema).json("/data/in.json");
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;
                        import org.apache.spark.sql.types.StructType;

                        class Reads {
                            void read(SparkSession spark, StructType schema, String mode, java.util.Map<String, String> opts) {
                                Dataset<Row> a = /*~~(%1$sfails the job; 2.4 read it as null.)~~>*/spark.read().option("mode", "FAILFAST").schema(schema).json("/data/in.json");
                                Dataset<Row> b = /*~~(%1$sdrops the whole row; 2.4 read it as null.)~~>*/spark.read().schema(schema).option("mode", "dropMalformed").json("/data/in.json");
                                Dataset<Row> c = /*~~(%1$sfills the corrupt-record column; 2.4 read it as null.)~~>*/spark.read().schema("n INT, _corrupt_record STRING").json("/data/in.json");
                                Dataset<Row> d = /*~~(%1$sfills the corrupt-record column; 2.4 read it as null.)~~>*/spark.read().option("columnNameOfCorruptRecord", "bad").schema(schema).json("/data/in.json");
                                Dataset<Row> e = /*~~(%1$sfails or drops the row under FAILFAST/DROPMALFORMED (the mode here is not a literal); 2.4 read it as null.)~~>*/spark.read().option("mode", mode).schema(schema).json("/data/in.json");
                                Dataset<Row> f = /*~~(%1$sfails or drops the row under FAILFAST/DROPMALFORMED (the mode here is not a literal); 2.4 read it as null.)~~>*/spark.read().options(opts).schema(schema).json("/data/in.json");
                            }
                        }
                        """.formatted(WHY)
                )
        );
    }

    @Test
    void flagsASchemaVariableInAFileThatNamesTheCorruptColumn() {
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
                                StructType schema = new StructType().add("n", DataTypes.IntegerType).add("_corrupt_record", DataTypes.StringType);
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
                                StructType schema = new StructType().add("n", DataTypes.IntegerType).add("_corrupt_record", DataTypes.StringType);
                                return /*~~(%1$sfills the corrupt-record column; 2.4 read it as null.)~~>*/spark.read().schema(schema).json("/data/in.json");
                            }
                        }
                        """.formatted(WHY)
                )
        );
    }

    @Test
    void leavesPermissiveAndInferredSchemaReadsUntouched() {
        // PERMISSIVE (the default) gives 2.4's row on 3.5.5 (probed).
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.SparkSession;
                        import org.apache.spark.sql.types.StructType;

                        class Reads {
                            void read(SparkSession spark, StructType schema) {
                                Dataset<Row> a = spark.read().schema(schema).json("/data/in.json");
                                Dataset<Row> b = spark.read().option("mode", "PERMISSIVE").schema(schema).json("/data/in.json");
                                Dataset<Row> c = spark.read().option("mode", "FAILFAST").json("/data/in.json");
                            }
                        }
                        """
                )
        );
    }
}
