package fix

import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.{OneHotEncoder => OHE, StringIndexer}
import org.apache.spark.ml.feature.OneHotEncoderModel
import org.apache.spark.sql.DataFrame

object OneHotEncoderEstimatorRename {
  def fit(df: DataFrame): OneHotEncoderModel = {
    val enc: OneHotEncoder = new OneHotEncoder()
      .setInputCols(Array("a"))
      .setOutputCols(Array("av"))
      .setDropLast(false)
    enc.fit(df)
  }
  def aliased(): OHE = new OHE().setHandleInvalid("keep")
  def loaded(p: String) = OneHotEncoder.load(p)
  def qualified() = new org.apache.spark.ml.feature.OneHotEncoder("uid")
}
