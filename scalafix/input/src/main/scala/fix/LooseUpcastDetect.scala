/*
rule = LooseUpcastDetect
 */
package fix

import org.apache.spark.sql.SparkSession

case class Widget(name: String)

// A minimal stand-in for the shape a JSON library (e.g. Play JSON's
// JsValue#as[T]) exposes -- same `.as[T]` syntax, nothing to do with Spark.
class JsValue {
  def as[T]: T = ???
}

object LooseUpcastDetectExample {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    val bad = Seq("str").toDF("value").as[Boolean] // assert: LooseUpcastDetect
    val ok = Seq(Widget("x")).toDF("name").as[Widget]

    // Not at risk: same `.as[Int]` syntax, unrelated receiver type.
    val json = new JsValue
    val count = json.as[Int]
  }
}
