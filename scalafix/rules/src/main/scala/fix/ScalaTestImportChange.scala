package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/** The suite styles and matchers ScalaTest 3.1 moved out of `org.scalatest`
 * (deprecated there in 3.1, removed in 3.2): name -> new path under
 * `org.scalatest`. spark-testing-base's Spark 3.5 release is built on
 * ScalaTest 3.2, so a suite using it needs every style moved, not only
 * FunSuite. `ScalaTestExtendsFix` renames the supertypes to match. */
object ScalaTestStyles {
  private val styles = Seq("FunSuite", "FlatSpec", "WordSpec", "FreeSpec", "FunSpec", "PropSpec", "FeatureSpec")

  val moved: Map[String, String] = styles.flatMap { style =>
    val pkg = style.toLowerCase
    Seq(style -> s"$pkg.Any$style", s"${style}Like" -> s"$pkg.Any${style}Like",
      s"Async$style" -> s"$pkg.Async$style", s"Async${style}Like" -> s"$pkg.Async${style}Like")
  }.toMap + ("Matchers" -> "matchers.should.Matchers")

  /** A supertype's new simple name: `FlatSpec` -> `AnyFlatSpec`; the Async styles keep theirs. */
  val renamed: Map[String, String] = moved.collect {
    case (from, to) if !from.startsWith("Async") && from != "Matchers" => from -> to.split('.').last
  }

  def importer(path: String): Importer =
    dialects.Scala212(s"import $path").parse[Stat].get.asInstanceOf[Import].importers.head
}

/**
 * Handle the import changes from ScalaTest 3.1
 * (see https://www.scalatest.org/release_notes/3.1.0). The extends-clause
 * renames (FunSuite/FunSuiteLike as a supertype) are handled by the sibling
 * `ScalaTestExtendsFix` instead -- see that file's doc comment.
 *
 * Previously matched each import as a WHOLE-STATEMENT quasiquote, e.g.
 * `q"""import org.scalatest.FunSuite"""`, which requires the import to have
 * EXACTLY that one importee. A grouped import --
 * `import org.scalatest.{FunSuite, Matchers}`, a very common style -- has a
 * 2-element importee list and structurally never matches that quasiquote, so
 * it was silently missed entirely, along with every other name sharing the
 * statement. There was also a real bug independent of that: the
 * `MustMatchers._` replacement was `"""...Matchers._\n"""` -- a
 * TRIPLE-QUOTED string doesn't interpret `\n` as an escape, so a real
 * occurrence would have spliced a literal two-character `\n` into the
 * rewritten source, which doesn't compile.
 *
 * Rewritten to iterate `Importer.importees` (the same pattern
 * `MigrateHiveContext`'s import-rewrite case already uses) so only the
 * matched importee is removed and replaced, regardless of what else shares
 * the import statement -- and `Patch.removeImportee` already collapses the
 * whole statement away cleanly when it was the only importee (verified by
 * `MigrateHiveContext`'s own fixture).
 */
class ScalaTestImportChange
    extends SemanticRule("ScalaTestImportChange") {
  override val description =
    """Handle the import change with ScalaTest ( see https://www.scalatest.org/release_notes/3.1.0 ) """

  override val isRewrite = true

  private val explanation = "ScalaTest 3.1 renamed/relocated this trait; see https://www.scalatest.org/release_notes/3.1.0"

  override def fix(implicit doc: SemanticDocument): Patch = {
    val utils = new Utils()

    def rewrite(i: Importee, replacement: Importer, newText: String): Patch =
      RuleFinding.report(
        RuleChange("ScalaTestImportChange", explanation, s"Rewrote to import $newText", i),
        List(Patch.removeImportee(i), utils.addImportIfNotPresent(replacement)).asPatch
      )

    doc.tree.collect {
      case Import(List(Importer(owner, importees))) =>
        val ownerText = owner.toString()
        importees.collect {
          case i @ Importee.Name(Name(name)) if ownerText == "org.scalatest" && ScalaTestStyles.moved.contains(name) =>
            val to = s"org.scalatest.${ScalaTestStyles.moved(name)}"
            rewrite(i, ScalaTestStyles.importer(to), to)
          case i @ Importee.Name(Name("FunSuite")) if ownerText == "org.scalatest.fixture" =>
            rewrite(i, importer"org.scalatest.funsuite.FixtureAnyFunSuite", "org.scalatest.funsuite.FixtureAnyFunSuite")
          case i @ Importee.Name(Name("MustMatchers")) if ownerText == "org.scalatest" =>
            rewrite(i, importer"org.scalatest.matchers.must.{Matchers => MustMatchers}", "org.scalatest.matchers.must.{Matchers => MustMatchers}")
          case i @ Importee.Wildcard() if ownerText == "org.scalatest.Matchers" =>
            rewrite(i, importer"org.scalatest.matchers.should.Matchers._", "org.scalatest.matchers.should.Matchers._")
          case i @ Importee.Wildcard() if ownerText == "org.scalatest.MustMatchers" =>
            rewrite(i, importer"org.scalatest.matchers.must.Matchers._", "org.scalatest.matchers.must.Matchers._")
        }.asPatch
    }.asPatch
  }
}
