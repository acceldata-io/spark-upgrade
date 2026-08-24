package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Family H (SQL migration guide, 3.3 -> 3.4): the CSV datasource no longer
 * supports `BinaryType` at all. A hard break with no legacy config, so Tier 3
 * detect-only -- there is nothing to inject and no safe rewrite (the fix is a
 * schema/encoding decision, e.g. base64 the column).
 *
 * Detection follows the schema one hop. The first cut only looked for
 * `BinaryType` inside the `.csv(...)` receiver chain, which fails on the shape
 * real code actually uses -- the schema is almost always built into a `val` on a
 * previous line, so the chain contains only that val's NAME. Caught by the
 * testkit fixture, which was deliberately written the realistic way. So: if
 * `.schema(x)` is given a plain identifier, resolve `x` to its `Defn.Val` in the
 * same document and inspect THAT for BinaryType. Inline schemas still work
 * because the chain is searched too.
 *
 * Bounded to the same file on purpose -- a schema imported from another module
 * is out of reach, and guessing would trade a precise rule for a noisy one.
 */
class CsvBinaryTypeDetect extends SemanticRule("CsvBinaryTypeDetect") {
  override val description =
    "Flags a CSV read whose explicit schema declares BinaryType, which Spark 3.4 no longer supports for CSV."

  private val binaryTypeMatcher = SymbolMatcher.normalized("org.apache.spark.sql.types.BinaryType")

  private def namesBinaryType(t: Tree)(implicit doc: SemanticDocument): Boolean =
    t.collect {
      case n: Term.Name if n.value == "BinaryType" && binaryTypeMatcher.matches(n) => true
      case n: Type.Name if n.value == "BinaryType" && binaryTypeMatcher.matches(n) => true
    }.contains(true)

  /** Bodies of every `val` in this document, by name -- so a `.schema(mySchema)`
   * can be followed to where `mySchema` was actually built. */
  private def valBodies(implicit doc: SemanticDocument): Map[String, Term] =
    doc.tree.collect {
      case Defn.Val(_, List(Pat.Var(Term.Name(n))), _, body) => n -> body
    }.toMap

  private def schemaDeclaresBinary(recv: Tree)(implicit doc: SemanticDocument): Boolean = {
    if (namesBinaryType(recv)) return true
    lazy val vals = valBodies
    recv.collect {
      case Term.Apply(Term.Select(_, Term.Name("schema")), List(Term.Name(ref))) =>
        vals.get(ref).exists(namesBinaryType)
    }.contains(true)
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ Term.Apply(Term.Select(recv, Term.Name("csv")), args) if args.nonEmpty && schemaDeclaresBinary(recv) =>
        RuleFinding.report(
          RuleChange(
            "CsvBinaryTypeDetect",
            "This CSV read declares a BinaryType column. Spark 3.4 removed BinaryType support from the CSV " +
              "datasource entirely -- there is no legacy config that restores it.",
            "No auto-rewrite and no config; change the column's type (base64-encode it as a string, or move the " +
              "dataset to a binary-capable format such as Parquet).",
            t
          )
        )
    }.asPatch
  }
}
