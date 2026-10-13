package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Design review Part 2 Family D: in 2.4, `add_months(d, n)` returned the last
 * day of the target month when `d` was the last day of its month
 * (2019-02-28 + 1 -> 2019-03-31); 3.x keeps the day (2019-03-28). No config
 * restores it, but this expression, using only functions 2.4 already has,
 * gives 2.4's result on both versions (probed for dates, timestamps, strings,
 * Feb 29, ordinary days and null):
 *
 *   when(to_date(d) === last_day(d), last_day(add_months(d, n))).otherwise(add_months(d, n))
 *
 * It repeats `d` and `n`, so the call is rewritten only when repeating them
 * cannot change meaning: `d` is a column reference (col/column/$/'/df(..)/
 * df.col(..) with a literal name, or a plain identifier), optionally wrapped
 * in to_date/to_timestamp, and `n` is an Int literal or identifier (2.4 has
 * no add_months(Column, Column)). The rewrite also changes the auto-generated
 * column name (add_months(d, 1) -> CASE WHEN ...), so it is applied only where
 * that name is discarded: aliased, withColumn's value, or a filter/where/
 * orderBy/sort/join argument. Any other call is reported as
 * AddMonthsSnapDetect.Manual (Tier 3) with the expression to use, and left
 * as written.
 */
class AddMonthsSnapDetect extends SemanticRule("AddMonthsSnapDetect") {
  override val description =
    "Rewrites add_months(d, n) to keep Spark 2.4's month-end snap, which Spark 3.0 dropped."

  private val ruleId = "AddMonthsSnapDetect"
  private val manualId = s"$ruleId.Manual"

  private val addMonthsMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.add_months")
  private val lastDayMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.last_day")
  private val whenMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.when")
  private val colMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.col", "org.apache.spark.sql.functions.column")
  private val datasetColMatcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.col")
  private val toDateMatcher = SymbolMatcher.normalized("org.apache.spark.sql.functions.to_date", "org.apache.spark.sql.functions.to_timestamp")
  private val datasetType = SymbolMatcher.normalized("org.apache.spark.sql.Dataset", "org.apache.spark.sql.package.DataFrame")

  private val explanation =
    "In Spark 2.4, add_months(d, n) returned the last day of the target month when d was the last day of its month " +
      "(2019-02-28 + 1 month -> 2019-03-31); Spark 3.0 keeps the day of month (2019-03-28). No config restores this."

  private def isDataset(name: Term.Name)(implicit doc: SemanticDocument): Boolean =
    name.symbol.info.exists(_.signature match {
      case ValueSignature(TypeRef(_, sym, _)) => datasetType.matches(sym)
      case _ => false
    })

  // A column expression that is safe to write more than once.
  private def isColumnRef(d: Term)(implicit doc: SemanticDocument): Boolean = d match {
    case _: Term.Name => true
    case _: Lit.Symbol => true
    case Term.Interpolate(Term.Name("$"), _, Nil) => true
    case Term.Apply(colMatcher(_), List(Lit.String(_))) => true
    case Term.Apply(Term.Select(_: Term.Name, datasetColMatcher(_)), List(Lit.String(_))) => true
    case Term.Apply(df: Term.Name, List(Lit.String(_))) => isDataset(df)
    case _ => false
  }

  private def isSafeDate(d: Term)(implicit doc: SemanticDocument): Boolean = d match {
    case Term.Apply(toDateMatcher(_), List(inner)) => isColumnRef(inner)
    case Term.Apply(toDateMatcher(_), List(inner, Lit.String(_))) => isColumnRef(inner)
    case _ => isColumnRef(d)
  }

  private def isSafeMonths(n: Term): Boolean = n match {
    case _: Lit.Int | _: Term.Name => true
    case _ => false
  }

  // last_day(add_months(...)) is the same on both versions, which also keeps
  // this rule from rewriting its own output a second time...
  private def underLastDay(t: Term)(implicit doc: SemanticDocument): Boolean = t.parent.exists {
    case Term.Apply(lastDayMatcher(_), List(arg)) => arg eq t
    case _ => false
  }

  // ...together with this: the `.otherwise(add_months(d, n))` half of it.
  private def isRewriteOutput(t: Term)(implicit doc: SemanticDocument): Boolean = t.parent.exists {
    case Term.Apply(Term.Select(Term.Apply(whenMatcher(_), List(_, Term.Apply(lastDayMatcher(_), List(snapped)))), Term.Name("otherwise")), List(arg)) =>
      (arg eq t) && snapped.syntax == t.syntax
    case _ => false
  }

  private def isOwnedBy(t: Tree, owner: String)(implicit doc: SemanticDocument): Boolean =
    t.symbol.owner.value == owner

  private def isColumnApi(t: Tree)(implicit doc: SemanticDocument): Boolean =
    isOwnedBy(t, "org/apache/spark/sql/functions.") || isOwnedBy(t, "org/apache/spark/sql/Column#")

  private val aliasMethods = Set("as", "alias", "name")
  private val unnamedDatasetArgs = Set("filter", "where", "orderBy", "sort", "sortWithinPartitions", "join", "joinWith")

  // True when the Column expression containing `node` never becomes a named
  // output column, so the rewrite's different auto-generated name (CASE WHEN
  // ... instead of add_months(d, n)) cannot leak: it is aliased (.as/.alias/
  // .name), is withColumn's value, or is a filter/where/orderBy/sort/join
  // argument. Walks up through Column functions and operators only; anything
  // else (select, agg, a val, ...) is conservatively false.
  private def nameDiscarded(node: Tree)(implicit doc: SemanticDocument): Boolean = node.parent match {
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

  override def fix(implicit doc: SemanticDocument): Patch = {
    val utils = new Utils()
    doc.tree.collect {
      case t @ Term.Apply(fun @ addMonthsMatcher(_), List(d, n)) if !underLastDay(t) && !isRewriteOutput(t) =>
        // Reuse the call's own qualifier (functions., F.) when it has one.
        val prefix = fun match {
          case Term.Select(qual, _) => s"$qual."
          case _ => ""
        }
        val replacement =
          s"${prefix}when(${prefix}to_date($d) === ${prefix}last_day($d), ${prefix}last_day($t)).otherwise($t)"
        val aliasNote =
          "The rewritten column's auto-generated name is a CASE WHEN ... expression instead of add_months(...), so alias it " +
            "with .as(\"<the name 2.4 gave it>\") if the column name matters."
        if (isSafeDate(d) && isSafeMonths(n) && nameDiscarded(t)) {
          // A bare call gets the three functions imported unless functions._ is.
          val imports =
            if (prefix.nonEmpty) Patch.empty
            else List(
              importer"org.apache.spark.sql.functions.when",
              importer"org.apache.spark.sql.functions.to_date",
              importer"org.apache.spark.sql.functions.last_day"
            ).map(utils.addImportIfNotPresent).asPatch
          RuleFinding.report(
            RuleChange(ruleId, explanation, s"Rewrote to $replacement, which gives 2.4's result on Spark 3.x too.", t),
            Patch.replaceTree(t, replacement) + imports
          )
        } else if (isSafeDate(d) && isSafeMonths(n)) {
          RuleFinding.report(
            RuleChange(
              manualId,
              explanation,
              "Not rewritten: the tool could not rewrite this call safely, because the result may become a named output " +
                s"column and the rewrite changes that column's name. To keep 2.4's result use $replacement" +
                (if (prefix.isEmpty) " (from org.apache.spark.sql.functions). " else ". ") + aliasNote,
              t
            )
          )
        } else {
          RuleFinding.report(
            RuleChange(
              manualId,
              explanation,
              "Not rewritten: the tool could not rewrite this call safely, because its arguments are not simple enough to repeat. " +
                "To keep 2.4's result, bind the date column to a val d and the month count to a val n, then use " +
                "when(to_date(d) === last_day(d), last_day(add_months(d, n))).otherwise(add_months(d, n)) " +
                "(all from org.apache.spark.sql.functions). " + aliasNote,
              t
            )
          )
        }
    }.asPatch
  }
}
