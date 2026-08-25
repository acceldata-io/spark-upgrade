package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.meta.tokens.Token

/**
 * Not a Spark API change -- a Scala 2.11 -> 2.12 SOURCE break (code review
 * 2026-08-22 §8.2). Procedure syntax (`def f() { ... }`, no `=`) is
 * deprecated starting 2.12 and REMOVED entirely in 2.13.
 * `VersionRegistry`/`DepUpgrade.ScalaVersionOutdated` bump `scalaVersion` to
 * 2.12 (Tier 1, auto-applied) but nothing in this tool detects or fixes the
 * source-level fallout of that bump -- this is the first rule to close that
 * gap.
 *
 * The detection is genuinely syntactic (a `SyntacticRule`, matching
 * `ScalaTestExtendsFix`'s precedent), but NOT a simple tree-shape match, and
 * the wrong intuition here is a real trap: scalameta does NOT leave
 * `decltpe = None` for procedure syntax the way it does for an ordinary
 * `def f() = { ... }` with an inferred type. Verified empirically (parsing
 * `def logStart() { println(...) }` and inspecting the tree directly):
 * scalameta gives procedure syntax a SYNTHETIC `decltpe = Some(Type.Name("Unit"))`
 * with zero real tokens behind it (no `:`, no `Unit` text anywhere in
 * `d.tokens`) -- the opposite of what "no declared type" suggests. So a
 * `decltpe`-based check is backwards; the only real signal is the token
 * stream itself: procedure syntax has no `=` token anywhere between the end
 * of the parameter list and the body, ordinary methods (typed or not) always
 * do. `isProcedureSyntax` finds the boundary right after the last real
 * parameter (falling back to the method name's end for a `()`-only
 * signature, so a default parameter value's own `=` -- e.g.
 * `def f(x: Int = 5) { ... }` -- is correctly excluded from the search) and
 * checks for an `=` token strictly between that boundary and the body.
 */
class ProcedureSyntaxDetect extends SyntacticRule("ProcedureSyntaxDetect") {
  override val description =
    "Procedure syntax (def f() { ... }, no `=`) is deprecated in Scala 2.12 and removed in 2.13; rewrites to def f(): Unit = { ... }."
  override val isRewrite = true

  private val explanation =
    "Procedure syntax (a method body immediately following the parameter list, with no `=`) is deprecated " +
      "starting Scala 2.12 and removed entirely in 2.13. The compiler infers Unit here, but that inference " +
      "goes away with the syntax."

  private def isProcedureSyntax(d: Defn.Def): Boolean = {
    val boundary = d.paramss.flatten.lastOption.map(_.pos.end).getOrElse(d.name.pos.end)
    val bodyStart = d.body.pos.start
    !d.tokens.exists(t => t.is[Token.Equals] && t.pos.start >= boundary && t.pos.start < bodyStart)
  }

  // The last non-trivia token before the body -- the parameter list's
  // closing paren, for real procedure syntax. Inserting right AFTER this
  // token (rather than left of the body) preserves whatever whitespace
  // already sits between `)` and `{` instead of doubling it up.
  private def lastSignificantTokenBeforeBody(d: Defn.Def): Option[Token] = {
    val bodyStart = d.body.pos.start
    d.tokens.filter(_.pos.start < bodyStart).reverseIterator.find(t => !t.is[Token.Trivia])
  }

  override def fix(implicit doc: SyntacticDocument): Patch =
    doc.tree.collect {
      case d @ Defn.Def(_, _, _, _, _, _: Term.Block) if isProcedureSyntax(d) =>
        lastSignificantTokenBeforeBody(d) match {
          case Some(closeParen) =>
            RuleFinding.reportSyntactic(
              RuleChange("ProcedureSyntaxDetect", explanation, "Inserted \": Unit =\" before the body", d.body),
              Patch.addRight(closeParen, ": Unit =")
            )
          case None => Patch.empty
        }
    }.asPatch
}
