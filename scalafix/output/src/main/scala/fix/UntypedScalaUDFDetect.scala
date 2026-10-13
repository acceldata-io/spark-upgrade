package fix

import java.sql.Date

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DateType, IntegerType, LongType, StringType, StructType}

object UntypedScalaUDFDetectExample {
  // Rewritten to the typed UDF.
  val untyped = udf[java.lang.Integer, Int]((x: Int) => x) 
  val nullBody = udf[java.lang.Integer, Int]((x: Int) => if (x > 1) x else null) 
  val two = functions.udf[String, String, Int]((s: String, n: Int) => s * n) 
  val date = udf[java.sql.Date, Date]((d: Date) => new Date(d.getTime + 86400000L)) 
  val javaField = udf[java.lang.Long, Long]((x: Long) => x * 2) 

  // Reported as UntypedScalaUDFDetect.Manual and left as written.
  val f: Int => Int = _ + 1
  val notALambda = udf(f, IntegerType) 
  val untypedParam = udf((xs: Seq[Int]) => xs.size, IntegerType) 
  val structResult = udf((x: Int) => x, new StructType()) 

  val typed = udf((x: Int) => x + 1)
}
