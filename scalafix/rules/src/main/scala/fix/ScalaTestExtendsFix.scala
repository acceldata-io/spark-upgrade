package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

// Fix the extends with since the QQ matcher doesn't like it and I'm lazy.
//
// Previously matched ANY `Type.Name` in the file equal to "FunSuite" --
// zero positional context, so a locally-defined `trait FunSuite {}` with no
// relation to ScalaTest at all got its own declaration renamed to
// `AnyFunSuite` (this file's own checked-in fixture demonstrated exactly
// that). Narrowed to `Type.Name` occurring within an `Init` (i.e. only as a
// supertype in an extends/with clause), which is the only position this
// rename is ever meaningful for. Being a `SyntacticRule` (no SemanticDB),
// this still can't distinguish a real `extends FunSuite` (imported from
// org.scalatest) from a same-named local look-alike used as a supertype --
// that residual gap needs symbol resolution and would mean promoting to a
// `SemanticRule`.
//
// Also now covers FunSuiteLike -- previously handled by a separate, equally
// bare `q"""class $cls extends FunSuiteLike { $expr }"""` quasiquote in
// `ScalaTestImportChange`, which in addition to the same unchecked-name
// issue required EXACTLY one template parent and EXACTLY one body statement,
// so any realistic multi-parent (`extends FunSuiteLike with Matchers`) or
// multi-statement suite silently never matched. Consolidated here since both
// renames are the same shape and this rule's Init-scoped match already
// handles arbitrary parent counts.
class ScalaTestExtendsFix
    extends SyntacticRule("ScalaTestExtendsFix") {
  override val description =
    """Handle the change with ScalaTest ( see https://www.scalatest.org/release_notes/3.1.0 ) """

  override val isRewrite = true

  private val renames: Map[String, String] = Map(
    "FunSuite" -> "AnyFunSuite",
    "FunSuiteLike" -> "AnyFunSuiteLike"
  )

  override def fix(implicit doc: SyntacticDocument): Patch =
    doc.tree.collect { case Init(tpe, _, _) =>
      tpe.collect {
        case v: Type.Name if renames.contains(v.value) =>
          val replacement = renames(v.value)
          RuleFinding.reportSyntactic(
            RuleChange("ScalaTestExtendsFix", s"ScalaTest 3.1 renamed ${v.value} to $replacement.", s"Rewrote to $replacement", v),
            Patch.replaceTree(v, replacement)
          )
      }.asPatch
    }.asPatch
}
