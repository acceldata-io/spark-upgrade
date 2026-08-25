package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/**
 * Both matchers below used to be `SymbolMatcher.normalized("scala.concurrent.onFuture")`
 * -- not a real symbol (there is no top-level `scala.concurrent.onFuture`), so
 * neither case arm could ever match anything and this rule was 100% dead: it
 * never fired on a single real `Future#onFailure`/`Future#onSuccess` call.
 * Verified against the real 2.11.12 scala-library jar that the correct
 * symbols are the instance methods `Future#onFailure`/`Future#onSuccess`.
 *
 * The rewrite itself was also broken independent of the matcher: it spliced
 * a fixed "(ev) }" placeholder onto the call via `Patch.addRight` next to a
 * `Patch.replaceTree` of just the receiver+method-name span, which discarded
 * the original partial function's case clauses (`args`) entirely -- any real
 * handler body would have been silently thrown away, and the onFailure
 * branch hardcoded `case Error(ev)` even though `scala.util.Try` has no
 * `Error` case (it's `Failure`/`Success`). Rewritten to transplant each
 * original `case <pattern> [if <guard>] => <body>` into
 * `case Failure(<pattern>) [if <guard>] => <body>` (or `Success(...)` for
 * onSuccess), preserving the real handler logic, and replacing the whole
 * call in one patch instead of two overlapping ones.
 *
 * A second, more subtle bug was caught during a later tier-promotion review
 * (verified via `javap`): `Future#onFailure`/`#onSuccess` take a
 * `PartialFunction`, so an outcome the case clauses don't cover is simply
 * never invoked (`PartialFunction#isDefinedAt` gates it) -- but
 * `Future#onComplete` takes a plain, TOTAL `Function1[Try[T], U]`. A
 * `{ case Failure(e) => ... }` literal with no other case, used where a
 * total function is expected, still compiles (as "match may not be
 * exhaustive" warns), but THROWS `scala.MatchError` at runtime the moment
 * the wrapped future actually succeeds -- which the original `onFailure`
 * callback would have simply never fired for. `rewriteCases` now appends a
 * trailing `case _ => ()` so the rewritten callback stays total, matching
 * the original PartialFunction's silent-skip behavior on a non-matching
 * outcome instead of crashing.
 */
class OnFailureFix extends SemanticRule("onFailureFix") {
  // See https://stackoverflow.com/questions/62047662/value-onsuccess-is-not-a-member-of-scala-concurrent-futureany
  val onFailureFunMatch = SymbolMatcher.normalized("scala.concurrent.Future.onFailure")
  val onSuccessFunMatch = SymbolMatcher.normalized("scala.concurrent.Future.onSuccess")

  private def rewriteCases(cases: List[Case], wrapper: String): String = {
    val transplanted = cases.map {
      case Case(pat, None, body) => s"case $wrapper($pat) => $body"
      case Case(pat, Some(cond), body) => s"case $wrapper($pat) if $cond => $body"
    }
    // `onComplete` expects a total function, unlike the PartialFunction
    // `onFailure`/`onSuccess` took -- without this, an outcome none of the
    // transplanted cases cover (e.g. the future actually succeeding, for an
    // onFailure rewrite) throws MatchError at runtime instead of the
    // original's silent no-op.
    (transplanted :+ "case _ => ()").mkString(" ")
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    val utils = new Utils()
    doc.tree.collect {
      case ns @ Term.Apply(onFailureFunMatch(_), List(Term.PartialFunction(cases))) =>
        val future = ns.children(0).children(0)
        val rewrite = s"$future.onComplete { ${rewriteCases(cases, "Failure")} }"
        RuleFinding.report(
          RuleChange("onFailureFix", "Future#onFailure was removed; rewritten to onComplete { case Failure(ev) => ... }.", s"Rewrote to $rewrite", ns),
          List(Patch.replaceTree(ns, rewrite), utils.addImportIfNotPresent(importer"scala.util.Failure")).asPatch
        )
      case ns @ Term.Apply(onSuccessFunMatch(_), List(Term.PartialFunction(cases))) =>
        val future = ns.children(0).children(0)
        val rewrite = s"$future.onComplete { ${rewriteCases(cases, "Success")} }"
        RuleFinding.report(
          RuleChange("onFailureFix", "Future#onSuccess was removed; rewritten to onComplete { case Success(ev) => ... }.", s"Rewrote to $rewrite", ns),
          List(Patch.replaceTree(ns, rewrite), utils.addImportIfNotPresent(importer"scala.util.Success")).asPatch
        )
    }.asPatch
  }
}
