/*
rule = CompileBreakDetect
 */
package fix

import com.databricks.spark.avro._// assert: CompileBreakDetect
import org.apache.spark.sql.{DataFrame, SparkSession}

// `.avro` passed as a function is left as written, so the import stays.
object CompileBreakDetectAvroManual {
  def reader(s: SparkSession): String => DataFrame = s.read.avro// assert: CompileBreakDetect
  def read(s: SparkSession): DataFrame = s.read.avro("in")// assert: CompileBreakDetect
}
