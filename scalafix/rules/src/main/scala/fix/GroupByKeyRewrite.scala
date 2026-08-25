package fix
import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/**
 * Family F (SQL migration guide, 2.4 -> 3.0): Tier 1 auto-rewrite counterpart
 * to `GroupByKeyWarn` for the specific, syntactically-recognizable shapes
 * where the renamed grouping column ("value" -> "key") is referenced by a
 * string/symbol literal right after `.groupByKey(...).count()`.
 *
 * Every one of the five branches below decided eligibility PURELY by
 * identifier-name text (`"groupByKey".equals(grpByKeyName) && "toDS".equals(fName)
 * && "count".equals(oprName) && ...`), with no semantic/symbol check at all --
 * despite `SemanticDocument` being available. Because this rule REWRITES
 * source (unlike a detect-only rule), a false positive here doesn't just
 * over-report, it silently corrupts code: any unrelated class defining
 * methods literally named `toDS`/`groupByKey`/`count`/`withColumnRenamed`
 * chained in one of these shapes -- nothing to do with Spark -- would have
 * its `"value"` string literal silently rewritten to `"key"`. Verified
 * against the real 2.4.8 jar that both symbols exist as expected
 * (`Dataset#groupByKey`, `KeyValueGroupedDataset#count`). Each branch below
 * now additionally requires the `groupByKey` and `count` call names to
 * resolve to those exact Spark symbols before rewriting, closing that gap
 * while keeping the existing, already-tested structural shape matching.
 */
class GroupByKeyRewrite extends SemanticRule("GroupByKeyRewrite") {
  override val isRewrite = true

  private val groupByKeyMatcher = SymbolMatcher.normalized("org.apache.spark.sql.Dataset.groupByKey")
  private val countMatcher = SymbolMatcher.normalized("org.apache.spark.sql.KeyValueGroupedDataset.count")

  override def fix(implicit doc: SemanticDocument): Patch = {
    val grpByKey = "groupByKey"
    val funcToDS = "toDS"
    val agrFunCount = "count"
    val oprCol = "withColumnRenamed"
    val colNameOld = "value"
    val colNameNew = "key"
    val ruleId = "GroupByKeyRewrite"
    val explanation = "Since Spark 3.0, Dataset.groupByKey(...).toDS().count() names the grouping column \"key\" instead of \"value\"."

    def renamed(oldTree: Tree, replacement: String)(implicit doc: SemanticDocument): Patch =
      RuleFinding.report(
        RuleChange(ruleId, explanation, s"Rewrote to $replacement", oldTree),
        Patch.replaceTree(oldTree, replacement)
      )

    def matchOnTree(t: Tree): Patch = {
      t.collect {
        case Term.Apply(
              Term.Select(
                Term.Apply(
                  Term.Select(
                    Term.Apply(
                      Term.Select(
                        Term.Apply(
                          Term.Select(
                            _,
                            _ @Term.Name(fName)
                          ),
                          _
                        ),
                        gbk @ Term.Name(grpByKeyName)
                      ),
                      _
                    ),
                    cnt @ Term.Name(oprName)
                  ),
                  _
                ),
                _ @Term.Name(oprColumnName)
              ),
              List(oldColName @ Lit.String(valueOld), _)
            )
            if grpByKey
              .equals(grpByKeyName) && funcToDS.equals(fName) && agrFunCount
              .equals(oprName) && oprCol.equals(oprColumnName) && colNameOld
              .equals(valueOld) && groupByKeyMatcher.matches(gbk) && countMatcher.matches(cnt) =>
          renamed(oldColName, "\"".concat(colNameNew).concat("\""))
        case Term.Apply(
              Term.Select(
                Term.Apply(
                  Term.Select(
                    Term.Apply(
                      Term.Select(
                        Term.Apply(
                          Term.Select(
                            _,
                            _ @Term.Name(toDSName)
                          ),
                          _
                        ),
                        gbk @ Term.Name(grpByKeyName)
                      ),
                      _
                    ),
                    cnt @ Term.Name(countName)
                  ),
                  _
                ),
                _ @Term.Name(selectName)
              ),
              List(
                Term.Interpolate(
                  _ @Term.Name(cName),
                  List(colOld @ Lit.String(colOldName)),
                  _
                ),
                _
              )
            )
            if grpByKey
              .equals(grpByKeyName) && funcToDS.equals(toDSName) && agrFunCount
              .equals(countName) && "select".equals(selectName) && "$"
              .equals(cName) && "value".equals(colOldName) && groupByKeyMatcher.matches(gbk) && countMatcher.matches(cnt) =>
          renamed(colOld, colNameNew)
        case Term.Apply(
              Term.Select(
                Term.Apply(
                  Term.Select(
                    Term.Apply(
                      Term.Select(
                        Term.Apply(
                          Term.Select(
                            _,
                            _ @Term.Name(toDSName)
                          ),
                          _
                        ),
                        gbk @ Term.Name(groupByKeyName)
                      ),
                      _
                    ),
                    cnt @ Term.Name(countName)
                  ),
                  _
                ),
                _ @Term.Name(selectName)
              ),
              List(
                Term.Apply(
                  _ @Term.Name(colName),
                  List(oldNameColumn @ Lit.String(valueName))
                ),
                _
              )
            )
            if funcToDS.equals(toDSName) && grpByKey.equals(
              groupByKeyName
            ) && "count".equals(
              countName
            ) && "select".equals(selectName) && "col"
              .equals(colName) && colNameOld.equals(valueName) && groupByKeyMatcher.matches(gbk) && countMatcher.matches(cnt) =>
          renamed(oldNameColumn, "\"".concat(colNameNew).concat("\""))
        case Term.Apply(
              Term.Select(
                Term.Apply(
                  Term.Select(
                    Term.Apply(
                      Term.Select(
                        Term.Apply(
                          Term.Select(
                            _,
                            _ @Term.Name(toDSName)
                          ),
                          _
                        ),
                        gbk @ Term.Name(groupByKeyName)
                      ),
                      _
                    ),
                    cnt @ Term.Name(countName)
                  ),
                  _
                ),
                _ @Term.Name(selectName)
              ),
              List(
                oldNameColumn @ Lit.Symbol(valueName),
                _
              )
            )
            if funcToDS.equals(toDSName) && grpByKey.equals(
              groupByKeyName
            ) && "count".equals(
              countName
            ) && "select"
              .equals(selectName) && "'value".equals(valueName.toString()) && groupByKeyMatcher.matches(gbk) && countMatcher.matches(cnt) =>
          renamed(oldNameColumn, "'".concat(colNameNew))

        case Term.Apply(
              Term.Select(
                Term.Apply(
                  Term.Select(
                    Term.Apply(
                      Term.Select(
                        Term.Apply(
                          Term.Select(
                            _,
                            _ @Term.Name(toDSName)
                          ),
                          _
                        ),
                        gbk @ Term.Name(groupByKeyName)
                      ),
                      _
                    ),
                    cnt @ Term.Name(countName)
                  ),
                  _
                ),
                _ @Term.Name(withColumnName)
              ),
              List(
                _,
                Term.Apply(
                  _,
                  List(
                    Term.Apply(
                      _ @Term.Name(colName),
                      List(oldNameColumn @ Lit.String(valueName))
                    )
                  )
                )
              )
            )
            if funcToDS.equals(toDSName) && grpByKey.equals(
              groupByKeyName
            ) && "count".equals(
              countName
            ) && "withColumn".equals(withColumnName) && "col".equals(
              colName
            ) && "value".equals(valueName) && groupByKeyMatcher.matches(gbk) && countMatcher.matches(cnt) =>
          renamed(oldNameColumn, "\"".concat(colNameNew).concat("\""))
      }.asPatch
    }

    matchOnTree(doc.tree)
  }

}
