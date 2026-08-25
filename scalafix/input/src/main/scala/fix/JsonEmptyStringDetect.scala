/*
rule = JsonEmptyStringDetect
 */
package fix

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object JsonEmptyStringDetectExample {
  val schema = StructType(Seq(StructField("count", org.apache.spark.sql.types.IntegerType)))

  def run(spark: SparkSession): Unit = {
    val bad = spark.read.schema(schema).json("/data/a") // assert: JsonEmptyStringDetect

    // Not at risk: inferred schema -- every field holding "" is inferred as
    // a string, so there's nothing to reject.
    val ok = spark.read.json("/data/b")

    // Not at risk: a ".schema(...)"-NAMED call, but nested inside an
    // unrelated ARGUMENT of the chain, not the reader's own schema. The old
    // `recv.collect` (whole-subtree scan) would have matched this too.
    object cfg { def schema(x: Int): String = x.toString }
    val okNested = spark.read.option("tag", cfg.schema(1)).json("/data/c")
  }
}
