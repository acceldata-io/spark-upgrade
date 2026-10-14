/*
rule = CompileBreakDetect
 */
package fix

import org.apache.spark.annotation.InterfaceStability
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
import org.apache.spark.sql.functions.{currentRow, sum, unboundedFollowing, unboundedPreceding}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// Every site here has an exact 3.5.5 form (Tier 1).
@InterfaceStability.Evolving// assert: CompileBreakDetect
class CompileBreakRewriteAnnotated

object CompileBreakRewrite {
  val schema = StructType(Seq(StructField("a", StringType)))
  def enc = RowEncoder(schema)// assert: CompileBreakDetect
  def enc2 = RowEncoder.apply(schema)// assert: CompileBreakDetect

  def metrics(m: MulticlassMetrics): Double =
    m.precision +// assert: CompileBreakDetect
      m.recall +// assert: CompileBreakDetect
      m.fMeasure// assert: CompileBreakDetect

  def km = new KMeans().setK(3).setRuns(2)// assert: CompileBreakDetect
  def km2 = new KMeans()
    .setK(3)
    .setRuns(2)// assert: CompileBreakDetect
    .setMaxIterations(5)
  def kmTrain(d: RDD[Vector]) = KMeans.train(d, 2, 10, 3)// assert: CompileBreakDetect
  def kmTrain2(d: RDD[Vector], runs: Int) = KMeans.train(d, 2, 10, runs, KMeans.RANDOM, 7L)// assert: CompileBreakDetect

  def trees(m: GBTClassificationModel) = m.numTrees// assert: CompileBreakDetect
  def trees2(m: GBTRegressionModel) = m.numTrees// assert: CompileBreakDetect
  def attempt(s: StageInfo) = s.attemptId// assert: CompileBreakDetect
  def layers(m: MultilayerPerceptronClassificationModel): Int = m.layers.length// assert: CompileBreakDetect

  def save(m: LogisticRegressionModel, sq: SQLContext) = m.write.context(sq).save("x")// assert: CompileBreakDetect
  def load(sq: SQLContext) = LogisticRegressionModel.read.context(sq).load("x")// assert: CompileBreakDetect

  def w: Column = sum("x").over(org.apache.spark.sql.expressions.Window.orderBy("a").rangeBetween(// assert: CompileBreakDetect
    unboundedPreceding(),// assert: CompileBreakDetect
    currentRow()))// assert: CompileBreakDetect
  def w2 = org.apache.spark.sql.expressions.Window.orderBy("a").rangeBetween(// assert: CompileBreakDetect
    functions.currentRow(),// assert: CompileBreakDetect
    unboundedFollowing)// assert: CompileBreakDetect
}
