package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 2 / Class A, Family H (SQL migration guide, 2.4 -> 3.0): the JSON
 * datasource no longer accepts an empty string as a value for a non-String,
 * non-Binary field -- 2.4 read `""` as null, 3.0 raises a malformed-record
 * error.
 *
 * Narrowed to a `.json(...)` read whose receiver chain sets an explicit
 * `.schema(...)`, which is precisely when the change can bite: with an INFERRED
 * schema every field that ever holds `""` is inferred as a string, so there is
 * nothing to reject. An explicit schema is what makes an empty string land
 * against an int/date/boolean column. That keeps the rule syntactic and
 * high-precision instead of trying to decide which fields of an arbitrary
 * schema expression are non-string.
 *
 * Feeds `spark.sql.legacy.json.allowEmptyString.enabled`.
 *
 * `hasExplicitSchema` walks only the fluent chain's own receiver spine, not
 * `recv.collect` over the whole receiver subtree -- the latter would also
 * match a `.schema(...)`-named call nested inside some unrelated ARGUMENT
 * within the same chain (e.g. `.option("tag", cfg.schema(x).toString)`),
 * which has nothing to do with the reader's own schema.
 */
class JsonEmptyStringDetect extends SemanticRule("JsonEmptyStringDetect") {
  override val description =
    "Flags a JSON read with an explicit schema; from Spark 3.0 an empty string is rejected for non-string fields instead of read as null."

  private def hasExplicitSchema(recv: Tree): Boolean = recv match {
    case Term.Apply(Term.Select(_, Term.Name("schema")), _) => true
    case Term.Apply(inner, _) => hasExplicitSchema(inner)
    case Term.Select(inner, _) => hasExplicitSchema(inner)
    case _ => false
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(recv, Term.Name("json")), args) if args.nonEmpty && hasExplicitSchema(recv) =>
        RuleFinding.report(
          RuleChange(
            "JsonEmptyStringDetect",
            "JSON read with an explicit schema. From Spark 3.0 an empty string is no longer accepted as a value " +
              "for a non-String/non-Binary field -- 2.4 read it as null, 3.0 treats the record as malformed " +
              "(so under PERMISSIVE mode the row's other fields survive but this one does not).",
            "No auto-rewrite; clean the source data or widen the field to string, in preference to injecting " +
              "spark.sql.legacy.json.allowEmptyString.enabled.",
            t
          )
        )
    }.asPatch
  }
}
