package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class SparkEmptyCollectionTypeRewriteTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new SparkEmptyCollectionTypeRewrite());
    }

    @Test
    void castsWhereTheColumnNameIsDiscarded() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.functions;
                        import static org.apache.spark.sql.functions.*;

                        class Schemas {
                            Dataset<Row> defaults(Dataset<Row> df) {
                                return df
                                        .withColumn("tags", coalesce(col("tags"), functions.array()))
                                        .withColumn("attrs", map())
                                        .select(col("tags"), col("attrs"), array().as("empty"))
                                        .filter(size(array()).equalTo(0));
                            }
                        }
                        """,
                        """
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import org.apache.spark.sql.functions;
                        import org.apache.spark.sql.types.DataTypes;

                        import static org.apache.spark.sql.functions.*;

                        class Schemas {
                            Dataset<Row> defaults(Dataset<Row> df) {
                                return df
                                        .withColumn("tags", coalesce(col("tags"), functions.array().cast(DataTypes.createArrayType(DataTypes.StringType, false))))
                                        .withColumn("attrs", map().cast(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, false)))
                                        .select(col("tags"), col("attrs"), array().cast(DataTypes.createArrayType(DataTypes.StringType, false)).as("empty"))
                                        .filter(size(array().cast(DataTypes.createArrayType(DataTypes.StringType, false))).equalTo(0));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesNamedColumnsArgumentsAndExistingCasts() {
        rewriteRun(
                java(
                        """
                        import org.apache.spark.sql.Column;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        import static org.apache.spark.sql.functions.*;

                        class Schemas {
                            Dataset<Row> project(Dataset<Row> df) {
                                return df.select(array(), map(), array(col("a")), array().cast("array<int>").as("x"));
                            }

                            Column empty() {
                                return array();
                            }
                        }
                        """
                )
        );
    }
}
