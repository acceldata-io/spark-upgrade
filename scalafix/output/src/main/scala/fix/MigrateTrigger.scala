import scala.concurrent.duration._
import org.apache.spark._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.OutputMode

object MigrateTrigger {
  def boop(): Unit = {
    val sc = new SparkContext()
    val trigger = org.apache.spark.sql.streaming.Trigger.ProcessingTime(1.second) 
    val renamed = org.apache.spark.sql.streaming.Trigger.ProcessingTime("30 seconds") 
    val mode = OutputMode.Append()
  }

  // Not at risk: the current, still-valid replacement API -- same simple
  // name ("ProcessingTime"), but owned by Trigger, not the removed object.
  def okBoop(): Unit = {
    val trigger = Trigger.ProcessingTime(1.second)
  }
}
