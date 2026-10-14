/*
rule = CompileBreakDetect
 */
package fix

import org.apache.spark.{AccumulableParam, AccumulatorParam, SparkContext}
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.ml.classification.{LogisticRegressionModel, MultilayerPerceptronClassificationModel}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{currentRow, lit, unboundedFollowing, unboundedPreceding}

import scala.collection.mutable

// Input-only fixture: the breaks with no rewrite (Tier 3) and the shapes a
// rewriting row leaves as written (`.Manual`, Tier 3). Every API below is gone
// in Spark 3.5.5, so the `output` project cannot reference it. The untyped
// foreachPartition/foreachBatch lambdas do not compile on Scala 2.12 at all
// (they are in input211), nor does sun.misc.BASE64* on JDK 17.
object MaxParam extends AccumulatorParam[Int] {// assert: CompileBreakDetect
  def zero(i: Int): Int = 0
  def addInPlace(a: Int, b: Int): Int = math.max(a, b)
}

object CompileBreakDetect {
  def runs = 2
  def km(k: KMeans) = k.getRuns// assert: CompileBreakDetect
  def kmCall(k: KMeans) = k.setRuns(runs + 1)// assert: CompileBreakDetect
  def kmTrain(d: RDD[Vector]) = KMeans.train(d, 2, 10, runs + 1)// assert: CompileBreakDetect
  def kmTrainOk(d: RDD[Vector]) = KMeans.train(d, 2, 10, KMeans.RANDOM)

  def stability = classOf[InterfaceStability]// assert: CompileBreakDetect
  def saver(m: LogisticRegressionModel) = m.write.context _// assert: CompileBreakDetect

  // lit(n) bounds: lit(0) names the frame `0 FOLLOWING` where 0L gives CURRENT ROW.
  def w = Window.orderBy("a").rangeBetween(lit(-1), lit(1))// assert: CompileBreakDetect
  def w2 = Window.orderBy("a").rangeBetween(// assert: CompileBreakDetect
    unboundedPreceding(),// assert: CompileBreakDetect
    lit(0))
  def w3 = unboundedFollowing()// assert: CompileBreakDetect
  def w4 = currentRow()// assert: CompileBreakDetect
  def wOk = Window.orderBy("a").rangeBetween(Window.unboundedPreceding, Window.currentRow)

  def cost(m: KMeansModel, df: DataFrame) = m.computeCost(df)// assert: CompileBreakDetect
  def costOk(m: org.apache.spark.mllib.clustering.KMeansModel, r: RDD[Vector]) = m.computeCost(r)
  def hadoop = SparkHadoopUtil.get.conf// assert: CompileBreakDetect
  def images = ImageSchema.readImages("x")// assert: CompileBreakDetect
  def imagesOk = ImageSchema.imageSchema
  def mlpLayers(m: MultilayerPerceptronClassificationModel) = {
    import m._
    layers.length// assert: CompileBreakDetect
  }

  def accColl(sc: SparkContext) = sc.accumulableCollection(mutable.ArrayBuffer[Int]())// assert: CompileBreakDetect
  def accParam(sc: SparkContext,
      p: AccumulableParam[Int, Int]) =// assert: CompileBreakDetect
    sc.accumulable(0)(p)// assert: CompileBreakDetect
  def sgd(d: RDD[LabeledPoint]) = LogisticRegressionWithSGD.train(d, 10)// assert: CompileBreakDetect
  def sgd2 = new LinearRegressionWithSGD()// assert: CompileBreakDetect

  // Typed lambdas, a method passed with `_`, and Dataset.foreach compile on Scala 2.12: not flagged.
  def partitions(ds: Dataset[String]): Unit = ds.foreachPartition((it: Iterator[String]) => println(it.size))
  def f(it: Iterator[String]): Unit = println(it.size)
  def partitions2(ds: Dataset[String]): Unit = ds.foreachPartition(f _)
  def each(ds: Dataset[String]): Unit = ds.foreach(x => println(x))
  def batches(df: DataFrame): Unit = df.writeStream.foreachBatch((b: DataFrame, id: Long) => ()).start()
}
