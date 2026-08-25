/*
rule=MultiLineDatasetReadWarn
 */
package fix
import org.apache.spark.sql.{SparkSession, Dataset}

class MultiLineDatasetReadWarn {
  def inSource(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    // The documented Spark option key -- must be caught even though the
    // rule's own historical fixture (and its old lowercase-only check)
    // never exercised this exact, most-common spelling.
    val df = (sparkSession
      .read
      .format("csv")
      .option("multiLine", true) // assert: MultiLineDatasetReadWarn
    )

    // Case-insensitive at runtime in Spark, so must also be caught.
    val dfLower = (sparkSession
      .read
      .format("csv")
      .option("multiline", true) // assert: MultiLineDatasetReadWarn
    )

    val okDf = (sparkSession
      .read
      .format("csv")
    )

    // Not at risk: an unrelated option whose ARGUMENT name merely contains
    // "multiline" -- the old rule's `read.toString.contains("multiline")`
    // check would have matched this too.
    val multilineFlag = false
    val okDf2 = (sparkSession
      .read
      .format("csv")
      .option("header", multilineFlag)
    )

    val ds7 = Seq("test 1", "test 2", "test 3").toDF().groupBy("value").count()
  }

}
