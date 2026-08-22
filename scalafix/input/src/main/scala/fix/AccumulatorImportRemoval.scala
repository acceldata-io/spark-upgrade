/*
 rule=AccumulatorUpgrade
 */
import org.apache.spark.{Accumulator, SparkConf, SparkContext}// assert: AccumulatorUpgrade

object AccumulatorImportRemovalFactory {
  def createInvalidRecordAccumulator(sc: SparkContext): Accumulator[Long] = {// assert: AccumulatorUpgrade
    sc.accumulator(0L, "invalidRecords")// assert: AccumulatorUpgrade
  }
}
