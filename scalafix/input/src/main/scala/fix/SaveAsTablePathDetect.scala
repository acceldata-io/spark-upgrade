/*
rule = SaveAsTablePathDetect
 */
package fix

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SaveAsTablePathDetectExample {
  def run(spark: SparkSession, df: DataFrame, outPath: String, mode: String): Unit = {
    df.write.option("path", outPath).saveAsTable("sales.daily") // assert: SaveAsTablePathDetect
    df.write // assert: SaveAsTablePathDetect
      .format("parquet")
      .partitionBy("dt")
      .option("path", "/warehouse/ext/sales_daily")
      .saveAsTable("sales.daily")
    df.write.mode("errorifexists").options(Map("compression" -> "snappy", "path" -> outPath)).saveAsTable("sales.daily") // assert: SaveAsTablePathDetect
    df.write.mode(SaveMode.ErrorIfExists).option("path", outPath).saveAsTable("sales.daily") // assert: SaveAsTablePathDetect
    df.write.mode(mode).option("path", outPath).saveAsTable("sales.daily") // assert: SaveAsTablePathDetect

    // The writer, or its options, held in a val.
    val writer = df.write.format("orc").option("path", outPath)
    writer.saveAsTable("sales.daily") // assert: SaveAsTablePathDetect
    val tableOpts = Map("path" -> outPath)
    df.write.options(tableOpts).saveAsTable("sales.daily") // assert: SaveAsTablePathDetect

    // Not flagged: no path (a managed table), a mode that never reaches the
    // check, a Hive-format table, or a path option on the READER.
    df.write.saveAsTable("sales.managed")
    df.write.mode("overwrite").option("path", outPath).saveAsTable("sales.daily")
    df.write.mode(SaveMode.Append).option("path", outPath).saveAsTable("sales.daily")
    df.write.mode("ignore").option("path", outPath).saveAsTable("sales.daily")
    df.write.format("hive").option("path", outPath).saveAsTable("sales.daily")
    val raw = spark.read.option("path", "/landing/sales").format("parquet").load()
    raw.write.saveAsTable("sales.raw")
  }
}
