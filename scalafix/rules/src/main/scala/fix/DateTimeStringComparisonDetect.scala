package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A (SQL migration guide, 2.4 -> 3.0): a comparison between a
 * date/timestamp and a string now casts the STRING to date/timestamp; 2.4 cast
 * the date/timestamp to string and compared text (a timestamp under = and <=>
 * excepted). `ts > "2020-01-01"` kept midnight rows on 2.4 and drops them on
 * 3.x; `d === "2020-1-1"` matched nothing on 2.4.
 *
 * The rule cannot see column types, so it keys on the other side: a Column
 * comparison (resolved to org.apache.spark.sql.Column) whose operand is a
 * date-shaped string literal -- directly, via `lit("...")`, or through a
 * `val` holding either in the same file (as CsvBinaryTypeDetect follows a
 * schema val). Only the literal/operator pairs a probe showed can change are
 * flagged, the same predicate as the sqlfluff rule's
 * `date_string_comparison_changes` (spark-upgrade/sql): a literal that is not
 * a zero-padded yyyy-MM-dd changes under every operator; a zero-padded one only
 * under > and <= (the column on the left) and as between()'s upper bound. So
 * the common `col("dt") === "2020-01-01"` is not flagged. Embedded SQL text is
 * the sqlfluff rule's.
 *
 * Feeds `spark.sql.legacy.typeCoercion.datetimeToString.enabled`.
 */
class DateTimeStringComparisonDetect extends SemanticRule("DateTimeStringComparisonDetect") {
  override val description =
    "Flags a Column comparison with a date-shaped string literal whose result can differ on Spark 3.x, which casts the string to date/timestamp instead of the date/timestamp to string."

  // Each comparison as the SQL operator it is, the column on the left.
  private val operators = Map(
    "===" -> "=", "equalTo" -> "=", "=!=" -> "<>", "!==" -> "<>", "notEqual" -> "<>", "<" -> "<", "lt" -> "<",
    "<=" -> "<=", "leq" -> "<=", ">" -> ">", "gt" -> ">", ">=" -> ">=", "geq" -> ">=", "<=>" -> "<=>",
    "eqNullSafe" -> "<=>"
  )
  // The operator seen from the column's side when the literal is on the left.
  private val mirrored = Map("<" -> ">", ">" -> "<", "<=" -> ">=", ">=" -> "<=")

  private val dateString = """\d{4}-\d{1,2}-\d{1,2}(?:[ T]\d{1,2}:\d{1,2}(?::\d{1,2}(?:\.\d{1,9})?)?)?""".r
  private val canonicalDate = """\d{4}-\d{2}-\d{2}""".r

  /** Whether comparing a date or timestamp column to `literal` with `operator` can answer differently on 3.x. */
  private def changes(literal: String, operator: String): Boolean =
    dateString.pattern.matcher(literal).matches() &&
      (!canonicalDate.pattern.matcher(literal).matches() || Set(">", "<=", "BETWEEN_HI").contains(operator))

  private val litMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.lit")

  private def isColumnMethod(op: Term.Name)(implicit doc: SemanticDocument): Boolean =
    op.symbol.owner.value == "org/apache/spark/sql/Column#"

  /** Bodies of every `val` in this document, by name. */
  private def valBodies(implicit doc: SemanticDocument): Map[String, Term] =
    doc.tree.collect {
      case Defn.Val(_, List(Pat.Var(Term.Name(n))), _, body) => n -> body
    }.toMap

  private def stringLiteral(t: Term, vals: => Map[String, Term])(implicit doc: SemanticDocument): Option[String] = t match {
    case Lit.String(s) => Some(s)
    case Term.Apply(litMatcher(_), List(Lit.String(s))) => Some(s)
    case Term.Name(n) => vals.get(n).flatMap(stringLiteral(_, Map.empty))
    case _ => None
  }

  /** Each operand that may be a literal, with the operator seen from the column's side. */
  private def operands(recv: Term, op: String, args: List[Term]): List[(Term, String)] =
    if (op == "between") args.zip(List("BETWEEN_LO", "BETWEEN_HI"))
    else operators.get(op).toList.flatMap(sqlOp => args.map(_ -> sqlOp) :+ (recv -> mirrored.getOrElse(sqlOp, sqlOp)))

  override def fix(implicit doc: SemanticDocument): Patch = {
    lazy val vals = valBodies
    def changesHere(recv: Term, op: Term.Name, args: List[Term]): Boolean =
      isColumnMethod(op) && operands(recv, op.value, args).exists { case (operand, sqlOp) =>
        stringLiteral(operand, vals).exists(changes(_, sqlOp))
      }
    def finding(t: Tree): Patch =
      RuleFinding.report(
        RuleChange(
          "DateTimeStringComparisonDetect",
          "A column is compared with a date-shaped string in a way whose result can change. If the column is a date " +
            "or timestamp, Spark 3.0 casts the string to date/timestamp, where 2.4 cast the column to string and " +
            "compared text: ts > \"2020-01-01\" kept midnight rows on 2.4 and drops them on 3.x, and " +
            "d === \"2020-1-1\" matched nothing on 2.4.",
          "No auto-rewrite; the tool injects spark.sql.legacy.typeCoercion.datetimeToString.enabled=true to keep 2.4's " +
            "behaviour. Its cost: a date partition column compared to a string is then cast to string, so partition " +
            "pruning stops. To drop the config, compare against to_date(lit(...)) / to_timestamp(lit(...)) and check " +
            "the rows.",
          t
        )
      )
    doc.tree.collect {
      case t @ Term.ApplyInfix(lhs, op, _, args) if changesHere(lhs, op, args) => finding(t)
      case t @ Term.Apply(Term.Select(recv, op), args) if changesHere(recv, op, args) => finding(t)
    }.asPatch
  }
}
