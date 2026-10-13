package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.meta.tokens.Token

/**
 * Since Spark 3.1 a `path` option can no longer coexist with a path argument
 * to `.load(path)`/`.save(path)`. Purely syntactic: it fires when an
 * `.option("path", ...)` call or an `.options(Map("path" -> ...))` key and a
 * `.load(...)`/`.save(...)` call with arguments appear in the same receiver
 * chain.
 *
 * Tier 1 since 2026-10-13 for one path argument: 2.4.8's `load(path)` is
 * `option("path", path).load()`, so the argument overwrote the option, and
 * dropping the option gives 2.4's result (probed on 2.4.8 and 3.5.5: read
 * `option("path", p1).load(p2)` and `options(Map("path" -> p1, ...)).load(p2)`
 * read p2; `option("path", p1).save(p2)` wrote p2 only). The rewrite deletes a
 * literal-key `.option("path", v)` call, or the `"path"` pair of a literal
 * `options(Map(...))` (the whole call when it was the only pair), when it is on
 * the load/save call's own chain and `v` is safe to drop (a literal, a name or
 * a field). Anything else -- several path arguments, a `paths: _*` splice, a
 * path option elsewhere in the receiver, a value with a possible side effect
 * -- is reported as PathOptionConflictDetect.Config (Tier 2), which feeds
 * spark.sql.legacy.pathOptionBehavior.enabled.
 */
class PathOptionConflictDetect extends SemanticRule("PathOptionConflictDetect") {
  override val description =
    "Rewrites a `path` option coexisting with a path argument to load()/save(), which Spark 3.1 rejects, by dropping the option 2.4 ignored."

  private val ruleId = "PathOptionConflictDetect"
  private val configId = s"$ruleId.Config"

  // `Map("path" -> v, ...)` desugars to `Term.ApplyInfix`; a plain tuple
  // literal `Map(("path", v), ...)` is a `Term.Tuple` -- either is a real
  // "path" KEY, unlike a bare `Lit.String("path")` anywhere in the map's
  // arguments (which also matches "path" used as some OTHER key's VALUE).
  private def pathValue(t: Tree): Option[Term] = t match {
    case Term.ApplyInfix(Lit.String("path"), Term.Name("->" | "→"), _, List(v)) => Some(v)
    case Term.Tuple(List(Lit.String("path"), v)) => Some(v)
    case _ => None
  }

  private def isPathKeyPair(t: Tree): Boolean = pathValue(t).isDefined

  private def hasPathOption(recv: Tree): Boolean =
    recv.collect {
      case Term.Apply(Term.Select(_, Term.Name("option")), List(Lit.String("path"), _)) => true
      case Term.Apply(Term.Select(_, Term.Name("options")), List(Term.Apply(_, pairs))) =>
        pairs.exists(isPathKeyPair)
    }.contains(true)

  // A value whose evaluation 2.4 discarded and which has no side effect to lose.
  private def safeToDrop(v: Term): Boolean = v match {
    case _: Lit | _: Term.Name => true
    case Term.Select(q, _) => safeToDrop(q)
    case Term.Interpolate(_, _, args) => args.forall(safeToDrop)
    case _ => false
  }

  private sealed trait PathSite
  private final case class OptionCall(call: Term.Apply, qual: Term, value: Term) extends PathSite
  private final case class OptionsMap(call: Term.Apply, qual: Term, map: Term.Apply, pairs: List[Term]) extends PathSite

  // The receiver chain of the load/save call: each `.f(...)` call's qualifier, down to the root.
  private def spine(t: Term): List[Term] = t match {
    case app @ Term.Apply(Term.Select(q, _), _) => app :: spine(q)
    case sel @ Term.Select(q, _) => sel :: spine(q)
    case app @ Term.Apply(fun: Term.Apply, _) => app :: spine(fun)
    case other => List(other)
  }

  private def pathSite(t: Term): Option[PathSite] = t match {
    case call @ Term.Apply(Term.Select(q, Term.Name("option")), List(Lit.String("path"), v)) => Some(OptionCall(call, q, v))
    case call @ Term.Apply(Term.Select(q, Term.Name("options")), List(map @ Term.Apply(Term.Name("Map"), pairs)))
        if pairs.exists(isPathKeyPair) => Some(OptionsMap(call, q, map, pairs))
    case _ => None
  }

  // Deletes `.f(...)` from `qual.f(...)`: every token after `qual` up to the end of the call,
  // including a line break before the dot.
  private def removeCall(call: Term.Apply, qual: Term)(implicit doc: SemanticDocument): Patch = {
    val tokens = call.tokens.filter(tok => tok.start >= qual.pos.end)
    Patch.removeTokens(tokens)
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(recv, name @ (Term.Name("load") | Term.Name("save"))), args)
          if args.nonEmpty && hasPathOption(recv) =>
        val why =
          s"A `path` option and a path argument to .${name.value}(...) are on the same chain; Spark 3.1+ rejects this, " +
            "where 2.4 used the argument and ignored the option."
        val sites = spine(recv).flatMap(pathSite)
        val pathOptionsOnSpine = sites.map {
          case OptionCall(_, _, _) => 1
          case OptionsMap(_, _, _, pairs) => pairs.count(isPathKeyPair)
        }.sum
        val pathOptionsInRecv = recv.collect {
          case Term.Apply(Term.Select(_, Term.Name("option")), List(Lit.String("path"), _)) => 1
          case Term.Apply(Term.Select(_, Term.Name("options")), List(Term.Apply(_, pairs))) => pairs.count(isPathKeyPair)
        }.sum
        val values = sites.flatMap {
          case OptionCall(_, _, v) => List(v)
          case OptionsMap(_, _, _, pairs) => pairs.flatMap(pathValue)
        }
        val singlePath = args match {
          case List(Term.Repeated(_)) => false
          case List(_) => true
          case _ => false
        }
        if (singlePath && pathOptionsOnSpine == pathOptionsInRecv && values.forall(safeToDrop)) {
          val patch = sites.map {
            case OptionCall(call, q, _) => removeCall(call, q)
            case OptionsMap(call, q, map, pairs) =>
              val rest = pairs.filterNot(isPathKeyPair)
              if (rest.isEmpty) removeCall(call, q)
              else Patch.replaceTree(map, s"${map.fun}(${rest.map(_.syntax).mkString(", ")})")
          }.asPatch
          RuleFinding.report(
            RuleChange(ruleId, why,
              s"Removed the `path` option: 2.4 read/wrote ${args.head}, the argument, and so does the rewritten call.", t),
            patch
          )
        } else {
          RuleFinding.report(
            RuleChange(
              configId,
              why,
              "Not rewritten: the tool rewrites only one path argument with a literal `path` option on the same chain. " +
                "Drop the `path` option (2.4 used the argument), or keep both and inject " +
                "spark.sql.legacy.pathOptionBehavior.enabled=true.",
              t
            )
          )
        }
    }.asPatch
  }
}
