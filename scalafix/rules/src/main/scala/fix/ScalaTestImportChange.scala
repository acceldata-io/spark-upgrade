package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class ScalaTestImportChange
    extends SemanticRule("ScalaTestImportChange") {
  override val description =
    """Handle the import change with ScalaTest ( see https://www.scalatest.org/release_notes/3.1.0 ) """

  override val isRewrite = true

  private val explanation = "ScalaTest 3.1 renamed/relocated this trait; see https://www.scalatest.org/release_notes/3.1.0"

  override def fix(implicit doc: SemanticDocument): Patch = {

    def report(t: Tree, newText: String): Patch =
      RuleFinding.report(RuleChange("ScalaTestImportChange", explanation, s"Rewrote to $newText", t), Patch.replaceTree(t, newText))

    def matchOnTree(t: Tree): Patch = {
      t match {
        case q"""import org.scalatest.FunSuite""" =>
          report(t, q"""import org.scalatest.funsuite.AnyFunSuite""".toString())
        case q"""class $cls extends FunSuite { $expr }""" =>
          report(t, f"class $cls extends AnyFunSuite { $expr }")
        case q"""import org.scalatest.FunSuiteLike""" =>
          report(t, q"""import org.scalatest.funsuite.AnyFunSuiteLike""".toString())
        case q"""class $cls extends FunSuiteLike { $expr }""" =>
          report(t, q"class $cls extends AnyFunSuiteLike { $expr }".toString)
        case q"""import org.scalatest.AsyncFunSuite""" =>
          report(t, q"""import org.scalatest.funsuite.AsyncFunSuiteLike""".toString())
        case q"""import org.scalatest.fixture.FunSuite""" =>
          report(t, q"""import org.scalatest.funsuite.FixtureAnyFunSuite""".toString())
        case q"""import org.scalatest.Matchers._""" =>
          report(t, q"""import org.scalatest.matchers.should.Matchers._""".toString())
        case q"""import org.scalatest.Matchers""" =>
          report(t, q"""import org.scalatest.matchers.should.Matchers""".toString())
        case q"""import org.scalatest.MustMatchers""" =>
          report(t, q"""import org.scalatest.matchers.must.{Matchers => MustMatchers}""".toString)
        case q"""import org.scalatest.MustMatchers._""" =>
          report(t, """import org.scalatest.matchers.must.Matchers._\n""")
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ =>
              elem.children.map(matchOnTree).asPatch
          }
      }
    }

    matchOnTree(doc.tree)
  }
}
