package fix
import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/**
 * Family D (SQL migration guide, 2.4 -> 3.0): since Spark 3.0, Column.name/
 * Column.as always propagate the underlying NamedExpression's metadata
 * instead of freezing it at call-time (`explicitMetadata`). The escape hatch
 * is the 2-arg `as(alias: String, metadata: Metadata)` overload, which is a
 * distinct overload from the 1-arg form matched below -- so requiring
 * exactly one String argument both targets the at-risk shape and excludes
 * the already-fixed one, with no separate suppression needed.
 *
 * Previous version matched any file that merely mentioned
 * `org.apache.spark.sql.types.Metadata` and contained identifiers named
 * `select`/`as` ANYWHERE in the file -- true even for unrelated `val`s named
 * `select`/`as` with no Spark Column code at all -- while never flagging the
 * actual at-risk pattern (`.as(alias)`/`.name(alias)` with no metadata),
 * since a file exhibiting that pattern usually never references `Metadata`.
 * This version matches call-site symbols on `Column`, so it fires on the
 * real risk and ignores same-named methods on unrelated receivers (e.g.
 * `Dataset.as`, which sets a join alias and has nothing to do with column
 * metadata).
 */
class MetadataWarnQQ extends SemanticRule("MetadataWarnQQ") {
  override val description =
    "Flags Column.as(alias)/Column.name(alias) with no explicit Metadata; Spark 3.0 propagates metadata from the underlying expression instead of freezing it at call time."

  private val asOrNameMatcher = SymbolMatcher.normalized(
    "org.apache.spark.sql.Column.as",
    "org.apache.spark.sql.Column.name"
  )

  private val explanation =
    "In Spark 3.0, the column metadata will always be propagated in the API Column.name and Column.as. " +
      "In Spark version 2.4 and earlier, the metadata of NamedExpression is set as the explicitMetadata " +
      "for the new column at the time the API is called, it won't change even if the underlying " +
      "NamedExpression changes metadata. To restore the behavior before Spark 3.0, you can use the API " +
      "as(alias: String, metadata: Metadata) with explicit metadata."

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(_, asOrNameMatcher(_)), List(_: Lit.String)) =>
        RuleFinding.report(RuleChange("MetadataWarnQQ", explanation, "No auto-rewrite; review manually.", t))
    }.asPatch
}
