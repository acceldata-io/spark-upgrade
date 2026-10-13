/*
rule = UntypedScalaUDFDetect
 */
package fix

import java.sql.Date

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DateType, IntegerType, LongType, StringType, StructType}

object UntypedScalaUDFDetectExample {
  // Rewritten to the typed UDF.
  val untyped = udf((x: Int) => x, IntegerType) // assert: UntypedScalaUDFDetect
  val nullBody = udf((x: Int) => if (x > 1) x else null, IntegerType) // assert: UntypedScalaUDFDetect
  val two = functions.udf((s: String, n: Int) => s * n, StringType) // assert: UntypedScalaUDFDetect
  val date = udf((d: Date) => new Date(d.getTime + 86400000L), DateType) // assert: UntypedScalaUDFDetect
  val javaField = udf((x: Long) => x * 2, DataTypes.LongType) // assert: UntypedScalaUDFDetect

  // Reported as UntypedScalaUDFDetect.Manual and left as written.
  val f: Int => Int = _ + 1
  val notALambda = udf(f, IntegerType) // assert: UntypedScalaUDFDetect
  val untypedParam = udf((xs: Seq[Int]) => xs.size, IntegerType) // assert: UntypedScalaUDFDetect
  val structResult = udf((x: Int) => x, new StructType()) // assert: UntypedScalaUDFDetect

  val typed = udf((x: Int) => x + 1)
}
