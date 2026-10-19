package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._
import scala.util.Try

/**
 * Plumbing for spark-migrate-cli, not a migration finding: reports the SQL text
 * of every `SparkSession.sql`/`SQLContext.sql` argument, so the CLI lints all of
 * a repo's SQL in one batched sqlfluff run with the linter the Java and PySpark
 * paths share (`SqlLiteralLinter`), and applies the audited SQL rewrites itself.
 * The counterpart of the Java recipe `SparkSqlLiteralExtract`.
 *
 * The finding's explanation is the kind, its change the SQL text:
 *  - `literal`: text known whole -- a string literal, `.stripMargin` or `.trim`
 *    of one, or a `val` defined in this file holding one.
 *  - `template <n>`: an `s`/`f`/`raw` interpolation, a `+` concatenation or a
 *    `.format(...)`, with each of its n run-time parts written `__param__` --
 *    or `0` where a format conversion says it is a number (`%d`, `%.2f`), so
 *    `LIMIT $n%d` still parses. A part that is a `val` of this file holding a
 *    literal is filled in.
 *  - `dynamic`: built at run time (a parameter, a `var`, a method's result);
 *    the text is empty.
 */
class SparkSqlExtract extends SemanticRule("SparkSqlExtract") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val sqlMatcher = SymbolMatcher.normalized(
      "org.apache.spark.sql.SparkSession.sql",
      "org.apache.spark.sql.SQLContext.sql"
    )
    // A `val` is bound once, so its text is the text at every use.
    val vals: Map[Symbol, Term] = doc.tree.collect {
      case Defn.Val(_, List(Pat.Var(name)), _, rhs) => name.symbol -> rhs
    }.toMap

    def report(arg: Term): Patch = {
      val (kind, text) = SparkSqlExtract.read(arg, name => vals.get(name.symbol), 0) match {
        case Some(sql) if sql.text.replace(SparkSqlExtract.Param, "").trim.nonEmpty =>
          (if (sql.holes == 0) "literal" else s"template ${sql.holes}", sql.text)
        case _ => ("dynamic", "")
      }
      RuleFinding.report(RuleChange("SparkSqlExtract", kind, text, arg))
    }

    // `sql` passed as a function over a collection: `ddl.foreach(spark.sql)`, `.map(spark.sql(_))`,
    // `.foreach(q => spark.sql(q))`. Each statement of a collection this file spells out is read where it is
    // written (the 2026-10-19 user trial: a CHAR(2) column in a `Seq` of DDL strings was never linted).
    def sqlFunction(fn: Term): Option[Tree] = fn match {
      case s @ Term.Select(_, Term.Name("sql")) if sqlMatcher.matches(s) => Some(s)
      case a @ Term.Apply(sqlMatcher(_), List(Term.Placeholder())) => Some(a)
      case Term.AnonymousFunction(a @ Term.Apply(sqlMatcher(_), List(Term.Placeholder()))) => Some(a)
      case Term.Function(List(p), a @ Term.Apply(sqlMatcher(_), List(n: Term.Name))) if n.value == p.name.value => Some(a)
      case Term.Block(List(f: Term.Function)) => sqlFunction(f)
      case _ => None
    }
    val overCollections: Seq[(Term, Tree, Option[Seq[Term]])] = doc.tree.collect {
      case Term.Apply(Term.Select(coll, Term.Name("foreach" | "map" | "flatMap")), List(fn)) if sqlFunction(fn).isDefined =>
        (fn, sqlFunction(fn).get, SparkSqlExtract.elements(coll, name => vals.get(name.symbol), 0))
    }
    val inner = overCollections.map(_._2.pos).toSet

    (doc.tree.collect {
      case a @ Term.Apply(sqlMatcher(_), List(arg)) if !inner.contains(a.pos) => report(arg)
    } ++ overCollections.map {
      case (_, _, Some(statements)) => statements.map(report).asPatch
      case (fn, _, None) => RuleFinding.report(RuleChange("SparkSqlExtract", "dynamic", "", fn))
    }).asPatch
  }
}

object SparkSqlExtract {

  /** What stands in for a part only known at run time. */
  val Param = "__param__"

  final case class Sql(text: String, holes: Int) {
    def ++(o: Sql): Sql = Sql(text + o.text, holes + o.holes)
  }

  private val hole = Sql(Param, 1)
  private val numberHole = Sql("0", 1)
  private val maxDepth = 3

  // A java.util.Formatter specifier, as String.format and the f interpolator read it.
  private val formatSpec = """%(\d+\$)?[-#+ 0,(<]*\d*(\.\d+)?[a-zA-Z%]""".r
  private val numberConversions = "doxXeEfgGa".toSet

  /** The text of `t`, with `Param` for each part known only at run time; None
   * when nothing of it is known. `valOf` is the right-hand side of the `val`
   * a name refers to, when this file defines one. */
  def read(t: Term, valOf: Term.Name => Option[Term], depth: Int): Option[Sql] = t match {
    case Lit.String(s) => Some(Sql(s, 0))
    case Term.Select(inner, Term.Name("stripMargin")) => read(inner, valOf, depth).map(s => s.copy(text = s.text.stripMargin))
    case Term.Apply(Term.Select(inner, Term.Name("stripMargin")), List(Lit.Char(c))) =>
      read(inner, valOf, depth).map(s => s.copy(text = s.text.stripMargin(c)))
    case Term.Select(inner, Term.Name("trim")) => read(inner, valOf, depth).map(s => s.copy(text = s.text.trim))
    case Term.Interpolate(Term.Name(prefix @ ("s" | "f" | "raw")), parts, args) =>
      val texts = parts.map { case Lit.String(p) => p; case p => p.syntax }.map { p =>
        val dollars = p.replace("$$", "$")
        if (prefix == "raw") dollars else Try(StringContext.treatEscapes(dollars)).getOrElse(dollars)
      }
      // The f interpolator's format for each argument follows it: `$x%.2f`.
      val specs = texts.tail.map(p => if (prefix == "f") formatSpec.findPrefixOf(p).filter(_ != "%%") else None)
      val pieces = texts.head +: texts.tail.zip(specs).map { case (p, spec) => p.drop(spec.fold(0)(_.length)) }
      val filled = args.zip(specs).map { case (a, spec) =>
        val k = known(a, valOf, depth)
        if (k.holes > 0 && spec.exists(m => numberConversions(m.last))) numberHole else k
      }
      val joined = pieces.zip(filled :+ Sql("", 0)).map { case (p, a) => Sql(if (prefix == "f") p.replace("%%", "%") else p, 0) ++ a }
      Some(joined.reduce(_ ++ _))
    case Term.ApplyInfix(lhs, Term.Name("+"), _, List(rhs)) =>
      val (l, r) = (read(lhs, valOf, depth), read(rhs, valOf, depth))
      // String concatenation only: one side must hold some text.
      if (l.isEmpty && r.isEmpty) None else Some(l.getOrElse(known(lhs, valOf, depth)) ++ r.getOrElse(known(rhs, valOf, depth)))
    case Term.Apply(Term.Select(fmt, Term.Name("format")), args) if args.nonEmpty =>
      read(fmt, valOf, depth).map { s =>
        var holes = s.holes
        val text = formatSpec.replaceAllIn(s.text, m => m.matched.last match {
          case '%' => "%"
          case 'n' => "\n"
          case c => holes += 1; if (numberConversions(c)) "0" else Param
        })
        Sql(text, holes)
      }
    case name: Term.Name if depth < maxDepth => valOf(name).flatMap(read(_, valOf, depth + 1))
    case _ => None
  }

  /** The statements of a collection spelled out in this file: `Seq(...)`, `List(...)`, `Array(...)`,
   * `Vector(...)`, a `val` holding one, or two joined with `++`. None when any part is not. */
  def elements(t: Term, valOf: Term.Name => Option[Term], depth: Int): Option[Seq[Term]] = t match {
    case Term.Apply(Term.Name("Seq" | "List" | "Array" | "Vector" | "IndexedSeq"), args) => Some(args)
    case Term.ApplyInfix(l, Term.Name("++"), _, List(r)) =>
      for (a <- elements(l, valOf, depth); b <- elements(r, valOf, depth)) yield a ++ b
    case name: Term.Name if depth < maxDepth => valOf(name).flatMap(elements(_, valOf, depth + 1))
    case _ => None
  }

  /** An interpolated or concatenated part: its text when known whole, else a hole. */
  private def known(t: Term, valOf: Term.Name => Option[Term], depth: Int): Sql = t match {
    case Lit.Int(v) => Sql(v.toString, 0)
    case Lit.Long(v) => Sql(v.toString, 0)
    case _ => read(t, valOf, depth).filter(_.holes == 0).getOrElse(hole)
  }
}
