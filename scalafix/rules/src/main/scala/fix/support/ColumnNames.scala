package fix.support

import scalafix.v1._

import scala.meta._

/**
 * Spark names an unaliased output column after its expression's SQL text, so
 * a rewrite that changes a Column expression also changes that generated name
 * (`select(array())` is `array()`, `select(array().cast("array<string>"))` is
 * `CAST(array() AS ARRAY<STRING>)`): written output and by-name references
 * change with it. An expression-level rewrite may therefore only fire where
 * the generated name is discarded; `nameDiscarded` decides that.
 */
object ColumnNames {

  private def isOwnedBy(t: Tree, owner: String)(implicit doc: SemanticDocument): Boolean =
    t.symbol.owner.value == owner

  private def isColumnApi(t: Tree)(implicit doc: SemanticDocument): Boolean =
    isOwnedBy(t, "org/apache/spark/sql/functions.") || isOwnedBy(t, "org/apache/spark/sql/Column#")

  private val aliasMethods = Set("as", "alias", "name")
  private val unnamedDatasetArgs = Set("filter", "where", "orderBy", "sort", "sortWithinPartitions", "join", "joinWith")

  /** True when the Column expression containing `node` never becomes a named
   *  output column: it is aliased (.as/.alias/.name), is withColumn's value, or
   *  is a filter/where/orderBy/sort/sortWithinPartitions/join/joinWith
   *  argument. Walks up through Column functions and operators only; anything
   *  else (select, agg, a val, ...) is conservatively false. */
  def nameDiscarded(node: Tree)(implicit doc: SemanticDocument): Boolean = node.parent match {
    case Some(sel @ Term.Select(qual, name)) if qual eq node =>
      if (aliasMethods(name.value) && isOwnedBy(name, "org/apache/spark/sql/Column#")) true
      else isColumnApi(name) && nameDiscarded(sel)
    case Some(app @ Term.Apply(fun, _)) if fun eq node => nameDiscarded(app)
    case Some(app @ Term.Apply(fun, args)) if args.exists(_ eq node) =>
      fun match {
        case Term.Select(_, name) if isOwnedBy(name, "org/apache/spark/sql/Dataset#") =>
          unnamedDatasetArgs(name.value) || (name.value == "withColumn" && (args.last eq node))
        case _ => isColumnApi(fun) && nameDiscarded(app)
      }
    case Some(infix @ Term.ApplyInfix(_, op, _, _)) => isColumnApi(op) && nameDiscarded(infix)
    case _ => false
  }
}
