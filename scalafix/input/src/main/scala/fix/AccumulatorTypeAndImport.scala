/*
 rule=AccumulatorUpgrade
 */
import org.apache.spark.{Accumulator, SparkConf, SparkContext}// assert: AccumulatorUpgrade

object AccumulatorFactory {
  def createLongAccumulator(sc: SparkContext): Accumulator[Long] = {// assert: AccumulatorUpgrade
    sc.accumulator(0L, "invalidRecords")// assert: AccumulatorUpgrade
  }

  def createIntAccumulator(sc: SparkContext): Accumulator[Int] = {// assert: AccumulatorUpgrade
    sc.accumulator(0, "otherRecords")// assert: AccumulatorUpgrade
  }
}
