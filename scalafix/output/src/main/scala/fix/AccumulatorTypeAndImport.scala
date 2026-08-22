import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorFactory {
  def createLongAccumulator(sc: SparkContext): org.apache.spark.util.LongAccumulator = {
    sc.longAccumulator("invalidRecords")
  }

  def createIntAccumulator(sc: SparkContext): Any = {
    /*sc.accumulator(0, "otherRecords")*/ null
  }
}
