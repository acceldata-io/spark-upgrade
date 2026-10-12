import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorFactory {
  def createLongAccumulator(sc: SparkContext): org.apache.spark.util.LongAccumulator = {
    sc.longAccumulator("invalidRecords")
  }
}
