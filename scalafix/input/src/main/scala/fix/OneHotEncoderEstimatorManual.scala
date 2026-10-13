/*
rule = OneHotEncoderEstimatorRename
 */
package fix

import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderEstimator}// assert: OneHotEncoderEstimatorRename

// Input-only fixture: the file also uses 2.4's old OneHotEncoder Transformer
// (removed in 3.0), so every OneHotEncoderEstimator reference is reported as
// OneHotEncoderEstimatorRename.Manual and left as written.
object OneHotEncoderEstimatorManual {
  val old = new OneHotEncoder().setInputCol("a").setOutputCol("av")
  val est = new OneHotEncoderEstimator()// assert: OneHotEncoderEstimatorRename
}
