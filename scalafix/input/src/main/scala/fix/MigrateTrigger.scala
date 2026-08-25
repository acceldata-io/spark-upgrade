/*
 rule=MigrateTrigger
 */
import scala.concurrent.duration._
import org.apache.spark._
import org.apache.spark.sql.streaming._

object MigrateTrigger {
  def boop(): Unit = {
    val sc = new SparkContext()
    val trigger = ProcessingTime(1.second) // assert: MigrateTrigger
  }

  // Not at risk: the current, still-valid replacement API -- same simple
  // name ("ProcessingTime"), but owned by Trigger, not the removed object.
  def okBoop(): Unit = {
    val trigger = Trigger.ProcessingTime(1.second)
  }
}
