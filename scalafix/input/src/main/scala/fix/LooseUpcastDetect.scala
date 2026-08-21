/*
rule = LooseUpcastDetect
 */
package fix

import org.apache.spark.sql.SparkSession

case class Widget(name: String)

object LooseUpcastDetectExample {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    val bad = Seq("str").toDF("value").as[Boolean] // assert: LooseUpcastDetect
    val ok = Seq(Widget("x")).toDF("name").as[Widget]
  }
}
