package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.meta.tokens.Token

/**
 * Not a Spark API change -- a Scala 2.11 -> 2.12 SOURCE break. Procedure syntax
 * (`def f() { ... }`, no `=`) compiles on 2.12 without a warning, even with
 * -deprecation (probed); 2.13 deprecates it and Scala 3 drops it. The rewrite
 * is the explicit form, which compiles on every version. A migration bumps
 * `scalaVersion` to 2.12, but nothing in these rules detected or fixed the
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
    "Procedure syntax (def f() { ... }, no `=`) still compiles on Scala 2.12 but is deprecated in 2.13 and dropped in Scala 3; rewrites to def f(): Unit = { ... }."
  override val isRewrite = true

  private val explanation =
    "Procedure syntax (a method body immediately following the parameter list, with no `=`) still compiles " +
      "on Scala 2.12, but is deprecated in 2.13 and dropped in Scala 3. The explicit `: Unit =` form means the " +
      "same and compiles everywhere."

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
