package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A detector (Family G):
 * since Spark 3.0, `spark.sql.analyzer.failAmbiguousSelfJoin` fails an
 * ambiguous self-join at analysis time instead of silently resolving column
 * references (sometimes to the wrong side). Narrowed to `df.join(df, ...)`
 * where the join receiver and the first argument (the other side of every
 * `Dataset.join` overload) resolve to the exact same symbol.
 *
 * Narrowed further by a probe on 2.4.8 and 3.5.5 with default
 * configs: what fails on 3.5.5 is a column referenced through the frame --
 * `df("n") > df("n")` in the condition, `.select(df("v"))` or
 * `.filter(df("n") > 15)` after the join -- where 2.4 silently resolved it to
 * one side. A join on column names (`df.join(df, "id")`,
 * `df.join(df, Seq("id"), "left")`) and a same-name equality
 * (`df("id") === df("id")`) give 2.4's rows and schema, and `.drop(df("v"))`
 * after the join gives 2.4's columns. So the join is not flagged when its
 * condition is one of those shapes and nothing later in the enclosing method
 * references a column through the frame (other than inside `.drop(...)`).
 *
 * Feeds `spark.sql.analyzer.failAmbiguousSelfJoin`, but the registry's own
 * remediation note prefers aliasing one side (e.g. `df.as("a").join(df.as("b"), ...)`)
 * over disabling the check.
 */
class SelfJoinAmbiguousColumnDetect extends SemanticRule("SelfJoinAmbiguousColumnDetect") {
  override val description =
    "Flags df.join(df, ...) self-joins using the exact same DataFrame reference on both sides, where a column is referenced through that frame -- which Spark 3.0 fails at analysis time as ambiguous."

  private val joinMatcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.join")

  /** Both sides name the same frame. Only a plain name counts: two calls (`conv.as("v")`,
   *  `eligible.as("t")`) share the method's symbol, not a frame, and
   *  an aliased side is the fix itself. */
  private def sameReference(a: Tree, b: Tree)(implicit doc: SemanticDocument): Boolean = (a, b) match {
    case (x: Term.Name, y: Term.Name) =>
      val sa = x.symbol
      sa != Symbol.None && sa == y.symbol
    case _ => false
  }

  /** `df("c")`, `df.col("c")` or `df.apply("c")` on the frame `frame`: the column's name. */
  private def columnRef(t: Tree, frame: Symbol)(implicit doc: SemanticDocument): Option[String] = t match {
    case Term.Apply(n: Term.Name, List(Lit.String(c))) if n.symbol == frame => Some(c)
    case Term.Apply(Term.Select(n: Term.Name, Term.Name("col" | "apply")), List(Lit.String(c))) if n.symbol == frame => Some(c)
    case _ => None
  }

  private def isColumnName(t: Term): Boolean = t match {
    case _: Lit.String => true
    case Term.Apply(Term.Name("Seq" | "List" | "Array"), cols) => cols.nonEmpty && cols.forall(_.isInstanceOf[Lit.String])
    case _ => false
  }

  /** A condition the probe showed 3.5.5 resolves as 2.4 did: column names, or
   *  same-name equalities `df("c") === df("c")` joined by `&&`. */
  private def safeCondition(t: Term, frame: Symbol)(implicit doc: SemanticDocument): Boolean = t match {
    case _ if isColumnName(t) => true
    case Term.ApplyInfix(l, Term.Name("&&"), _, List(r)) => safeCondition(l, frame) && safeCondition(r, frame)
    case Term.ApplyInfix(l, Term.Name("==="), _, List(r)) =>
      columnRef(l, frame).exists(c => columnRef(r, frame).contains(c))
    case _ => false
  }

  /** A column referenced through `frame` after `join` in its method (or
   *  template), other than as a `.drop(...)` argument. */
  private def laterReference(join: Tree, frame: Symbol)(implicit doc: SemanticDocument): Boolean = {
    val scope = Iterator.iterate(join.parent)(_.flatMap(_.parent)).takeWhile(_.isDefined).flatten
      .collectFirst { case d: Defn.Def => d; case t: Template => t }
      .getOrElse(doc.tree)
    def inDrop(t: Tree): Boolean = t.parent.exists {
      case Term.Apply(Term.Select(_, Term.Name("drop")), args) => args.exists(_ eq t)
      case _ => false
    }
    scope.collect {
      case t: Term if t.pos.start >= join.pos.end && columnRef(t, frame).isDefined && !inDrop(t) => ()
    }.nonEmpty
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(recv, joinMatcher(_)), args) if args.nonEmpty && sameReference(recv, args.head) =>
        val frame = recv.symbol
        val safe = args.lift(1).exists(safeCondition(_, frame)) && !laterReference(t, frame)
        if (safe) Patch.empty
        else
          RuleFinding.report(
            RuleChange(
              "SelfJoinAmbiguousColumnDetect",
              "df.join(df, ...) joins a DataFrame to itself using the exact same reference on both sides, and a column is referenced through that frame (in the condition or after the join); Spark 3.0's analyzer fails this as an ambiguous self-join where 2.4 silently resolved it to one side.",
              "No auto-rewrite; alias one side (e.g. df.as(\"a\").join(df.as(\"b\"), ...)) instead of injecting spark.sql.analyzer.failAmbiguousSelfJoin=false.",
              t
            )
          )
    }.asPatch
  }
}
