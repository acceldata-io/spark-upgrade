import org.apache.spark._

object BadAcc {
  def boop(): Unit = {
    val sc = new SparkContext()
    val litLongAcc = sc.longAccumulator
    val litDoubleAcc = sc.doubleAccumulator
    val namedLongAcc = sc.longAccumulator("cheese")
    val namedDoubleAcc = sc.doubleAccumulator("cheese")
    val rdd = sc.parallelize(List(1,2,3))
  }
}
