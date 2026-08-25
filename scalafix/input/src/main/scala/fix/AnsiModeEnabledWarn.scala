/*
rule=AnsiModeEnabledWarn
 */
package fix

import org.apache.spark.sql.SparkSession

object AnsiModeEnabledWarn {
  def inSource(): Unit = {
    val spark = SparkSession.builder // assert: AnsiModeEnabledWarn
      .appName("example")
      .getOrCreate()
    spark.stop()
  }

  // Not at risk: a local, unrelated object coincidentally also named
  // "SparkSession" with a "builder" member -- the old bare-name check would
  // have flagged this too.
  object Mocks {
    object SparkSession {
      def builder: String = "not the real thing"
    }
    val fake = SparkSession.builder
  }
}
