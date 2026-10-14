/*
rule = CompileBreakDetect
 */
// A stand-in for com.databricks:spark-avro_2.11 (no Scala 2.12 build exists),
// with the same package object and implicit classes, so the symbols match.
// CompileBreakDetectAvroManual uses it too.
package com.databricks.spark {
  import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

  package object avro {
    implicit class AvroDataFrameWriter[T](writer: DataFrameWriter[T]) {
      def avro: String => Unit = writer.format("com.databricks.spark.avro").save
    }
    implicit class AvroDataFrameReader(reader: DataFrameReader) {
      def avro: String => DataFrame = reader.format("com.databricks.spark.avro").load
    }
  }
}

package fix {
  import com.databricks.spark.avro._// assert: CompileBreakDetect
  import org.apache.spark.sql.{DataFrame, SparkSession}

  object CompileBreakDetectAvro {
    def read(s: SparkSession): DataFrame = s.read.avro("in")// assert: CompileBreakDetect
    def write(df: DataFrame): Unit = df.write.avro("out")// assert: CompileBreakDetect
    def writeMode(df: DataFrame): Unit = df.write.mode("overwrite").avro("out")// assert: CompileBreakDetect
    def ok(s: SparkSession): DataFrame = s.read.format("avro").load("in")
  }
}
