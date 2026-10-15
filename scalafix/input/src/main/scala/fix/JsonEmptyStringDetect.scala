/*
rule = JsonEmptyStringDetect
 */
package fix

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object JsonEmptyStringDetectExample {
  val schema = StructType(Seq(StructField("count", IntegerType)))
  val withCorrupt = StructType(Seq(StructField("count", IntegerType), StructField("_corrupt_record", StringType)))

  def run(spark: SparkSession, mode: String, opts: Map[String, String]): Unit = {
    // The malformed record is visible: the job fails, the row is dropped, or
    // the corrupt-record column fills.
    val failFast = spark.read.option("mode", "FAILFAST").schema(schema).json("/data/a") // assert: JsonEmptyStringDetect
    val dropped = spark.read.schema(schema).option("mode", "dropMalformed").json("/data/a") // assert: JsonEmptyStringDetect
    val viaMap = spark.read.options(Map("mode" -> "FAILFAST")).schema(schema).json("/data/a") // assert: JsonEmptyStringDetect
    val corrupt = spark.read.schema(withCorrupt).json("/data/a") // assert: JsonEmptyStringDetect
    val ddl = spark.read.schema("count INT, _corrupt_record STRING").json("/data/a") // assert: JsonEmptyStringDetect
    val named = spark.read.option("columnNameOfCorruptRecord", "bad").schema(schema).json("/data/a") // assert: JsonEmptyStringDetect
    // The mode cannot be read.
    val unknown = spark.read.option("mode", mode).schema(schema).json("/data/a") // assert: JsonEmptyStringDetect
    val opaque = spark.read.options(opts).schema(schema).json("/data/a") // assert: JsonEmptyStringDetect

    // Not at risk: PERMISSIVE (the default) nulls the field and keeps the
    // row's others, exactly as 2.4 did (probed on 2.4.8 and 3.5.5).
    val permissive = spark.read.schema(schema).json("/data/a")
    val explicit = spark.read.option("mode", "PERMISSIVE").schema(schema).json("/data/a")

    // Not at risk: inferred schema -- every field holding "" is inferred as
    // a string, so there's nothing to reject.
    val ok = spark.read.option("mode", "FAILFAST").json("/data/b")

    // Not at risk: a ".schema(...)"-NAMED call, but nested inside an
    // unrelated ARGUMENT of the chain, not the reader's own schema.
    object cfg { def schema(x: Int): String = x.toString }
    val okNested = spark.read.option("mode", "FAILFAST").option("tag", cfg.schema(1)).json("/data/c")
  }
}
