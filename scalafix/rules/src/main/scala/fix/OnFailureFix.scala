package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

class OnFailureFix extends SemanticRule("onFailureFix") {
  // See https://stackoverflow.com/questions/62047662/value-onsuccess-is-not-a-member-of-scala-concurrent-futureany
  val onFailureFunMatch = SymbolMatcher.normalized("scala.concurrent.onFuture")
  val onSuccessFunMatch = SymbolMatcher.normalized("scala.concurrent.onFuture")

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case ns @ Term.Apply(j @ onFailureFunMatch(f), args) =>
        val future = ns.children(0).children(0)
        val rewrite = List(
          Patch.addRight(j, "(ev) }"),
          Patch.replaceTree(j, s"${future}.onComplete { case Error(ev) => ")
        ).asPatch
        RuleFinding.report(RuleChange("onFailureFix", "Future#onFailure was removed; rewritten to onComplete { case Failure(ev) => ... }.", "Rewrote to onComplete", ns), rewrite)
      case ns @ Term.Apply(j @ onSuccessFunMatch(f), args) =>
        val future = ns.children(0).children(0)
        val rewrite = List(
          Patch.addRight(j, "(sv) }"),
          Patch.replaceTree(j, s"${future}.onComplete { case Success(sv) => ")
        ).asPatch
        RuleFinding.report(RuleChange("onFailureFix", "Future#onSuccess was removed; rewritten to onComplete { case Success(sv) => ... }.", "Rewrote to onComplete", ns), rewrite)
    }.asPatch
  }
}
