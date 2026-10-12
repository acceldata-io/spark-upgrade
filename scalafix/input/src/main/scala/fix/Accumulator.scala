/*
 rule=AccumulatorUpgrade
 */
import org.apache.spark._

object BadAcc {
  def boop(): Unit = {
    val sc = new SparkContext()
    val litLongAcc = sc.accumulator(0L)// assert: AccumulatorUpgrade
    val litDoubleAcc = sc.accumulator(0.0)// assert: AccumulatorUpgrade
    val namedLongAcc = sc.accumulator(0L, "cheese")// assert: AccumulatorUpgrade
    val namedDoubleAcc = sc.accumulator(0.0, "cheese")// assert: AccumulatorUpgrade
    val rdd = sc.parallelize(List(1,2,3))
  }
}
