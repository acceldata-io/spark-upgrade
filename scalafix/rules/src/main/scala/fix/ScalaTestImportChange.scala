package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

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
          case i @ Importee.Name(Name("FunSuite")) if ownerText == "org.scalatest" =>
            rewrite(i, importer"org.scalatest.funsuite.AnyFunSuite", "org.scalatest.funsuite.AnyFunSuite")
          case i @ Importee.Name(Name("FunSuiteLike")) if ownerText == "org.scalatest" =>
            rewrite(i, importer"org.scalatest.funsuite.AnyFunSuiteLike", "org.scalatest.funsuite.AnyFunSuiteLike")
          case i @ Importee.Name(Name("AsyncFunSuite")) if ownerText == "org.scalatest" =>
            rewrite(i, importer"org.scalatest.funsuite.AsyncFunSuiteLike", "org.scalatest.funsuite.AsyncFunSuiteLike")
          case i @ Importee.Name(Name("FunSuite")) if ownerText == "org.scalatest.fixture" =>
            rewrite(i, importer"org.scalatest.funsuite.FixtureAnyFunSuite", "org.scalatest.funsuite.FixtureAnyFunSuite")
          case i @ Importee.Name(Name("Matchers")) if ownerText == "org.scalatest" =>
            rewrite(i, importer"org.scalatest.matchers.should.Matchers", "org.scalatest.matchers.should.Matchers")
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
