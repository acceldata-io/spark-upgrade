/*
rule = EmptyCollectionTypeDetect
 */
package fix

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._

object EmptyCollectionTypeDetectExample {
  def shapes(df: DataFrame): Unit = {
    // Rewritten: the auto-generated column name is discarded.
    df.select(array().as("a")) // assert: EmptyCollectionTypeDetect
    df.withColumn("m", map()) // assert: EmptyCollectionTypeDetect
    df.withColumn("a", coalesce(col("arr"), array())) // assert: EmptyCollectionTypeDetect
    df.withColumn("a", when(col("c") > 0, array(col("s"))).otherwise(array())) // assert: EmptyCollectionTypeDetect
    df.filter(size(array()) === 0) // assert: EmptyCollectionTypeDetect

    // Reported as EmptyCollectionTypeDetect.Config: the name could leak.
    df.select(array(), map()) // assert: EmptyCollectionTypeDetect
  }
  val bad1 = array() // assert: EmptyCollectionTypeDetect
  val bad2 = functions.map() // assert: EmptyCollectionTypeDetect
  // Already cast: the type is explicit, not flagged.
  val cast = array().cast("array<int>")
  val ok = array(lit(1), lit(2))
}
