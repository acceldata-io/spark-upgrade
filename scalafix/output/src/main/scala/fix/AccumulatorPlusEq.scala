import org.apache.spark.SparkContext

object AccumulatorPlusEqExample {
  def run(sc: SparkContext): Unit = {
    val invalidRecordAccumulator: org.apache.spark.util.LongAccumulator = sc.longAccumulator("invalidRecords")
    invalidRecordAccumulator.add(3L)
  }
}
