/*
 rule=MigrateTrigger
 */
import scala.concurrent.duration._
import org.apache.spark._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.{ProcessingTime => PT, OutputMode}

object MigrateTrigger {
  def boop(): Unit = {
    val sc = new SparkContext()
    val trigger = ProcessingTime(1.second) // assert: MigrateTrigger
    val renamed = PT("30 seconds") // assert: MigrateTrigger
    val mode = OutputMode.Append()
  }

  // Not at risk: the current, still-valid replacement API -- same simple
  // name ("ProcessingTime"), but owned by Trigger, not the removed object.
  def okBoop(): Unit = {
    val trigger = Trigger.ProcessingTime(1.second)
  }
}
