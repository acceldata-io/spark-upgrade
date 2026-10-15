/*
rule = SelfJoinAmbiguousColumnDetect
 */
package fix

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SelfJoinAmbiguousColumnDetectExample {
  def failing(spark: SparkSession): Unit = {
    val df = spark.read.format("parquet").load("/data/a")
    // 3.5.5 fails each of these as ambiguous; 2.4 resolved them to one side.
    val a = df.join(df, df("n") > df("n")) // assert: SelfJoinAmbiguousColumnDetect
    val b = df.join(df, df("a") === df.col("b")) // assert: SelfJoinAmbiguousColumnDetect
    val c = df.join(df, "id").select(df("v")) // assert: SelfJoinAmbiguousColumnDetect
  }

  def laterReference(spark: SparkSession): Unit = {
    val df = spark.read.format("parquet").load("/data/a")
    val joined = df.join(df, Seq("id"), "left") // assert: SelfJoinAmbiguousColumnDetect
    joined.filter(df("n") > 15)
  }

  def opaqueCondition(spark: SparkSession): Unit = {
    val df = spark.read.format("parquet").load("/data/a")
    val cond = df("n") > df("n")
    val d = df.join(df, cond) // assert: SelfJoinAmbiguousColumnDetect
  }

  // Probed: the same rows and schema on 2.4.8 and 3.5.5. Each in its own
  // method, since any later df(...) reference in the method is flagged.
  def byName(spark: SparkSession): Unit = {
    val df = spark.read.format("parquet").load("/data/a")
    df.join(df, "id")
  }

  def byNames(spark: SparkSession): Unit = {
    val df = spark.read.format("parquet").load("/data/a")
    df.join(df, Seq("id", "v"), "left").groupBy("id").count()
  }

  def sameName(spark: SparkSession): Unit = {
    val df = spark.read.format("parquet").load("/data/a")
    df.join(df, df("id") === df("id") && df.col("v") === df.col("v"))
  }

  def dropped(spark: SparkSession): Unit = {
    val df = spark.read.format("parquet").load("/data/a")
    df.join(df, "id").drop(df("v")).select(col("id"))
  }

  def other(spark: SparkSession): Unit = {
    val df = spark.read.format("parquet").load("/data/a")
    df.join(spark.read.format("parquet").load("/data/b"), df("id") > 1)
  }
}
