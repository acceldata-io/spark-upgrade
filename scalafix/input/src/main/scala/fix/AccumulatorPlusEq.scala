/*
 rule=AccumulatorUpgrade
 */
import org.apache.spark.{Accumulator, SparkContext}// assert: AccumulatorUpgrade

object AccumulatorPlusEqExample {
  def run(sc: SparkContext): Unit = {
    val invalidRecordAccumulator: Accumulator[Long] = sc.accumulator(0L, "invalidRecords")// assert: AccumulatorUpgrade
    invalidRecordAccumulator += 3L// assert: AccumulatorUpgrade
  }
}
