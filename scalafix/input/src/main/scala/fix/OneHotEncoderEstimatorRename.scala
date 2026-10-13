/*
rule = OneHotEncoderEstimatorRename
 */
package fix

import org.apache.spark.ml.feature.OneHotEncoderEstimator// assert: OneHotEncoderEstimatorRename
import org.apache.spark.ml.feature.{OneHotEncoderEstimator => OHE, StringIndexer}// assert: OneHotEncoderEstimatorRename
import org.apache.spark.ml.feature.OneHotEncoderModel
import org.apache.spark.sql.DataFrame

object OneHotEncoderEstimatorRename {
  def fit(df: DataFrame): OneHotEncoderModel = {
    val enc: OneHotEncoderEstimator = new OneHotEncoderEstimator()// assert: OneHotEncoderEstimatorRename
      .setInputCols(Array("a"))
      .setOutputCols(Array("av"))
      .setDropLast(false)
    enc.fit(df)
  }
  def aliased(): OHE = new OHE().setHandleInvalid("keep")
  def loaded(p: String) = OneHotEncoderEstimator.load(p)// assert: OneHotEncoderEstimatorRename
  def qualified() = new org.apache.spark.ml.feature.OneHotEncoderEstimator("uid")// assert: OneHotEncoderEstimatorRename
}
