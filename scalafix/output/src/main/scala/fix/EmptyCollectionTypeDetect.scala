package fix

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ ArrayType, MapType, StringType }

object EmptyCollectionTypeDetectExample {
  def shapes(df: DataFrame): Unit = {
    // Rewritten: the auto-generated column name is discarded.
    df.select(array().cast(ArrayType(StringType, false)).as("a")) 
    df.withColumn("m", map().cast(MapType(StringType, StringType, false))) 
    df.withColumn("a", coalesce(col("arr"), array().cast(ArrayType(StringType, false)))) 
    df.withColumn("a", when(col("c") > 0, array(col("s"))).otherwise(array().cast(ArrayType(StringType, false)))) 
    df.filter(size(array().cast(ArrayType(StringType, false))) === 0) 

    // Reported as EmptyCollectionTypeDetect.Config: the name could leak.
    df.select(array(), map()) 
  }
  val bad1 = array() 
  val bad2 = functions.map() 
  // Already cast: the type is explicit, not flagged.
  val cast = array().cast("array<int>")
  val ok = array(lit(1), lit(2))
}
