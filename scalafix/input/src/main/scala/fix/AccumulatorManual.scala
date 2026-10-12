/*
 rule=AccumulatorUpgrade
 */
import org.apache.spark.{Accumulator, SparkContext}

// No exact typed accumulator for these: each is reported for a person and left
// as it is, so the file still compiles on 2.4 for the next Phase A rule. The
// Accumulator import stays while a usage still names the type. Input-only: the
// code still uses APIs Spark 3 removed.
object AccumulatorManual {
  def boop(sc: SparkContext): Unit = {
    val num = 0
    val numAcc = sc.accumulator(num)// assert: AccumulatorUpgrade
    val litAcc = sc.accumulator(0)// assert: AccumulatorUpgrade
    val namedAcc = sc.accumulator(0, "cheese")// assert: AccumulatorUpgrade
  }

  def createIntAccumulator(sc: SparkContext): Accumulator[Int] = {// assert: AccumulatorUpgrade
    sc.accumulator(0, "otherRecords")// assert: AccumulatorUpgrade
  }
}
