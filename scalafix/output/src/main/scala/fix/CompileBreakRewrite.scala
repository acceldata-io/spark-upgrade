package fix

import org.apache.spark.ml.classification.{GBTClassificationModel, LogisticRegressionModel, MultilayerPerceptronClassificationModel}
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Window

// Every site here has an exact 3.5.5 form (Tier 1).
@Evolving
class CompileBreakRewriteAnnotated

object CompileBreakRewrite {
  val schema = StructType(Seq(StructField("a", StringType)))
  def enc = ExpressionEncoder(schema)
  def enc2 = ExpressionEncoder.apply(schema)

  def metrics(m: MulticlassMetrics): Double =
    m.accuracy +
      m.accuracy +
      m.accuracy

  def km = new KMeans().setK(3)
  def km2 = new KMeans()
    .setK(3)
    .setMaxIterations(5)
  def kmTrain(d: RDD[Vector]) = KMeans.train(d, 2, 10)
  def kmTrain2(d: RDD[Vector], runs: Int) = KMeans.train(d, 2, 10, KMeans.RANDOM, 7L)

  def trees(m: GBTClassificationModel) = m.getNumTrees
  def trees2(m: GBTRegressionModel) = m.getNumTrees
  def attempt(s: StageInfo) = s.attemptNumber()
  def layers(m: MultilayerPerceptronClassificationModel): Int = m.getLayers.length

  def save(m: LogisticRegressionModel, sq: SQLContext) = m.write.session(sq.sparkSession).save("x")
  def load(sq: SQLContext) = LogisticRegressionModel.read.session(sq.sparkSession).load("x")

  def w: Column = sum("x").over(org.apache.spark.sql.expressions.Window.orderBy("a").rangeBetween(
    Window.unboundedPreceding,
    Window.currentRow))
  def w2 = org.apache.spark.sql.expressions.Window.orderBy("a").rangeBetween(
    Window.currentRow,
    Window.unboundedFollowing)
}
