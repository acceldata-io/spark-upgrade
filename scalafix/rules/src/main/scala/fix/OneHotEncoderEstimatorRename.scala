package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Spark 3.0 removed the old `OneHotEncoder` Transformer and renamed
 * `OneHotEncoderEstimator` to `OneHotEncoder`. javap on 3.5.5: the new class
 * is the same Estimator, with every 2.4.8 OneHotEncoderEstimator member
 * (inputCols, outputCols, dropLast, handleInvalid, their setters, fit ->
 * OneHotEncoderModel, read/load/write) plus single-column inputCol/outputCol.
 * Probed on 2.4.8 and 3.5.5: the same uid prefix, category sizes, output
 * columns and vectors, for the defaults and for dropLast=false with
 * handleInvalid=keep.
 *
 * Tier 1, 3.x-only output (`targetOnly`): in 2.4.8 `OneHotEncoder` is the old
 * Transformer, so the renamed code does not compile there. Every reference
 * named `OneHotEncoderEstimator` (type, constructor, companion, import) is
 * renamed; a renaming import (`{OneHotEncoderEstimator => OHE}`) keeps its
 * alias. If the file also uses 2.4's old `OneHotEncoder` Transformer, that code
 * needs a person (it is removed), and renaming would make the two collide, so
 * every reference is reported as OneHotEncoderEstimatorRename.Manual (Tier 3)
 * instead. A pipeline or estimator saved by 2.4 records the old class name and
 * does not load on 3.x either way.
 */
class OneHotEncoderEstimatorRename extends SemanticRule("OneHotEncoderEstimatorRename") {
  override val description =
    "Renames OneHotEncoderEstimator, removed in Spark 3.0, to OneHotEncoder, which is the same estimator in 3.x."

  private val ruleId = "OneHotEncoderEstimatorRename"
  private val manualId = s"$ruleId.Manual"

  private val estimator = SymbolMatcher.normalized("org.apache.spark.ml.feature.OneHotEncoderEstimator")
  private val oldTransformer = SymbolMatcher.normalized("org.apache.spark.ml.feature.OneHotEncoder")
  private val featurePackage = "org.apache.spark.ml.feature"

  private val explanation =
    "OneHotEncoderEstimator was renamed to OneHotEncoder in Spark 3.0, replacing the old OneHotEncoder Transformer. " +
      "The 3.x OneHotEncoder is the same estimator: the same parameters, setters and OneHotEncoderModel."

  override def fix(implicit doc: SemanticDocument): Patch = {
    val usesOldTransformer = doc.tree.collect {
      case n: Name if n.value == "OneHotEncoder" && oldTransformer.matches(n) && !n.parent.exists(_.isInstanceOf[Importee]) => n
    }.nonEmpty

    // Importers of org.apache.spark.ml.feature that import the old name too: the estimator's importee is removed there.
    def importsOldName(importer: Importer): Boolean =
      importer.importees.exists {
        case Importee.Name(Name.Indeterminate("OneHotEncoder")) => true
        case _ => false
      }

    def report(at: Tree, rewrite: Patch): Patch =
      if (usesOldTransformer)
        RuleFinding.report(
          RuleChange(manualId, explanation,
            "Not rewritten: this file also uses 2.4's old OneHotEncoder Transformer, which Spark 3.0 removed. Migrate that " +
              "code first, then rename OneHotEncoderEstimator to OneHotEncoder.", at)
        )
      else
        RuleFinding.report(RuleChange(ruleId, explanation, "Renamed OneHotEncoderEstimator to OneHotEncoder.", at), rewrite)

    doc.tree.collect {
      case importer @ Importer(ref, importees) if ref.syntax == featurePackage =>
        importees.collect {
          case i @ Importee.Name(Name.Indeterminate("OneHotEncoderEstimator")) =>
            report(i, if (importsOldName(importer)) Patch.removeImportee(i) else Patch.replaceTree(i, "OneHotEncoder"))
          case i @ Importee.Rename(Name.Indeterminate("OneHotEncoderEstimator"), alias) =>
            report(i, Patch.replaceTree(i, s"OneHotEncoder => $alias"))
        }.asPatch
      case n: Type.Name if n.value == "OneHotEncoderEstimator" && estimator.matches(n) =>
        report(n, Patch.replaceTree(n, "OneHotEncoder"))
      case n: Term.Name if n.value == "OneHotEncoderEstimator" && estimator.matches(n) && !n.parent.exists(_.isInstanceOf[Importer]) =>
        report(n, Patch.replaceTree(n, "OneHotEncoder"))
    }.asPatch
  }
}
