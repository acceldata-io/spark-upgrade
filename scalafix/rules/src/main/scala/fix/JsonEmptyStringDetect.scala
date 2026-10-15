package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A, Family H (SQL migration guide, 2.4 -> 3.0): the JSON
 * datasource no longer accepts an empty string as a value for a non-String,
 * non-Binary field -- 2.4 read `""` as null, 3.0 treats the record as
 * malformed.
 *
 * Narrowed to a `.json(...)` read whose receiver chain sets an explicit
 * `.schema(...)`, which is precisely when the change can bite: with an INFERRED
 * schema every field that ever holds `""` is inferred as a string, so there is
 * nothing to reject. An explicit schema is what makes an empty string land
 * against an int/date/boolean column.
 *
 * And narrowed (2026-10-15) to the modes where the malformed record is
 * visible. Probed on 2.4.8 and 3.5.5 (`{"a":"","b":1}` with `a INT`): in the
 * default PERMISSIVE mode 3.5.5 keeps the record's other fields and nulls
 * this one, exactly 2.4's row. What differs is FAILFAST (3.5.5 fails the
 * job), DROPMALFORMED (it drops the row) and a corrupt-record column (it
 * fills, where 2.4 left it null). So the read is flagged when its mode is one
 * of those two, when its schema names `_corrupt_record` or the chain sets
 * `columnNameOfCorruptRecord`, or when the mode cannot be read (a non-literal
 * mode, or `.options(...)` of something other than a literal Map) -- as the
 * PySpark rule of the same id does.
 *
 * Feeds `spark.sql.legacy.json.allowEmptyString.enabled`.
 *
 * The chain is walked along its own receiver spine only, not the whole
 * receiver subtree -- a `.schema(...)`-named call nested inside an unrelated
 * ARGUMENT (`.option("tag", cfg.schema(x).toString)`) is not the reader's
 * schema.
 */
class JsonEmptyStringDetect extends SemanticRule("JsonEmptyStringDetect") {
  override val description =
    "Flags a JSON read with an explicit schema in FAILFAST/DROPMALFORMED mode or with a corrupt-record column; from Spark 3.0 an empty string in a non-string field makes the record malformed instead of null."

  private val corruptColumn = "_corrupt_record"

  /** The calls of the fluent chain `recv` ends with: `name(args)`, outermost first. */
  private def spine(recv: Tree): List[(String, List[Term])] = recv match {
    case Term.Apply(Term.Select(inner, Term.Name(name)), args) => (name, args) :: spine(inner)
    case Term.Apply(inner, _) => spine(inner)
    case Term.Select(inner, _) => spine(inner)
    case _ => Nil
  }

  /** The reader options the chain sets: key (lower case) -> value, with None
   *  as the key for an `.options(...)` the rule cannot read. */
  private def options(calls: List[(String, List[Term])]): List[(Option[String], Term)] = calls.flatMap {
    case ("option", List(Lit.String(key), value)) => List(Some(key.toLowerCase) -> value)
    case ("options", List(Term.Apply(Term.Name("Map"), pairs))) =>
      pairs.map {
        case Term.ApplyInfix(Lit.String(key), Term.Name("->"), _, List(value)) => Some(key.toLowerCase) -> value
        case Term.Tuple(List(Lit.String(key), value)) => Some(key.toLowerCase) -> value
        case other => None -> other
      }
    case ("options", List(other)) => List(None -> other)
    case _ => Nil
  }

  private def mentionsCorrupt(t: Tree): Boolean = t.collect { case Lit.String(s) if s.contains(corruptColumn) => () }.nonEmpty

  /** The schema names the corrupt-record column: in the argument itself, or
   *  in the val the argument names. */
  private def corruptInSchema(schema: Term)(implicit doc: SemanticDocument): Boolean =
    mentionsCorrupt(schema) || (schema match {
      case name: Term.Name =>
        val sym = name.symbol
        sym != Symbol.None && doc.tree.collect {
          case Defn.Val(_, List(Pat.Var(v)), _, rhs) if v.symbol == sym => mentionsCorrupt(rhs)
        }.contains(true)
      case _ => false
    })

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(recv, Term.Name("json")), args) if args.nonEmpty =>
        val calls = spine(recv)
        calls.collectFirst { case ("schema", List(schema)) => schema } match {
          case Some(schema) =>
            val opts = options(calls)
            val mode = opts.collectFirst { case (Some("mode"), value) => value }
            val effect = mode match {
              case Some(Lit.String(m)) if m.equalsIgnoreCase("FAILFAST") => Some("fails the job")
              case Some(Lit.String(m)) if m.equalsIgnoreCase("DROPMALFORMED") => Some("drops the whole row")
              case _ if opts.exists(_._1.contains("columnnameofcorruptrecord")) || corruptInSchema(schema) =>
                Some("fills the corrupt-record column")
              case Some(_: Lit.String) => None
              case Some(_) => Some("fails or drops the row under FAILFAST/DROPMALFORMED (the mode here is not a literal)")
              case None if opts.exists(_._1.isEmpty) =>
                Some("fails or drops the row under FAILFAST/DROPMALFORMED (the options here are not a literal Map)")
              case None => None
            }
            effect.fold(Patch.empty) { e =>
              RuleFinding.report(
                RuleChange(
                  "JsonEmptyStringDetect",
                  "JSON read with an explicit schema: from Spark 3.0 an empty string in a non-string field (int, bigint, " +
                    s"boolean, decimal) makes the record malformed, which here $e; 2.4 read it as null.",
                  "No auto-rewrite; clean the source data or widen the field to string, or inject " +
                    "spark.sql.legacy.json.allowEmptyString.enabled.",
                  t
                )
              )
            }
          case None => Patch.empty
        }
    }.asPatch
  }
}
