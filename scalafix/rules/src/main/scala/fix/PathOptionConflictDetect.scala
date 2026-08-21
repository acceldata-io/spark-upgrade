package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A detector (design review Part 2 Family G / Part 4):
 * since Spark 3.1, a `path` option can no longer coexist with a path
 * argument to `.load(path)`/`.save(path)` -- purely syntactic and high
 * confidence: this only fires when both an `.option("path", ...)` call and
 * a non-empty-argument `.load(...)`/`.save(...)` call appear in the same
 * receiver chain, which scalameta's tree already makes trivial to find (the
 * `.option(...)` call is necessarily part of `.load`/`.save`'s own receiver
 * tree in a fluent chain).
 *
 * Feeds `spark.sql.legacy.pathOptionBehavior.enabled`, but the registry's
 * own remediation note prefers dropping one of the two path sources over
 * injecting the config.
 */
class PathOptionConflictDetect extends SemanticRule("PathOptionConflictDetect") {
  override val description =
    "Flags a `path` option coexisting with a path argument to load()/save(), which Spark 3.1 rejects instead of silently picking one."

  private def hasPathOption(recv: Tree): Boolean =
    recv.collect {
      case Term.Apply(Term.Select(_, Term.Name("option")), List(Lit.String("path"), _)) => true
      case Term.Apply(Term.Select(_, Term.Name("options")), args) =>
        args.exists(_.collect { case Lit.String("path") => true }.nonEmpty)
    }.contains(true)

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(recv, name @ (Term.Name("load") | Term.Name("save"))), args)
          if args.nonEmpty && hasPathOption(recv) =>
        RuleFinding.report(
          RuleChange(
            "PathOptionConflictDetect",
            s"A `path` option and a path argument to .${name.value}(...) coexist on the same chain; Spark 3.1+ rejects this instead of silently choosing one.",
            "No auto-rewrite; drop the `path` option or the path argument instead of injecting spark.sql.legacy.pathOptionBehavior.enabled.",
            t
          )
        )
    }.asPatch
  }
}
