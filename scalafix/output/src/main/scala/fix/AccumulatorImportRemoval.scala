import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorImportRemovalFactory {
  def createInvalidRecordAccumulator(sc: SparkContext): org.apache.spark.util.LongAccumulator = {
    sc.longAccumulator("invalidRecords")
  }
}
