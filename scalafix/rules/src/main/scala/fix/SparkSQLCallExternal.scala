package fix

import java.io._


import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._
import scala.util.Try
import sys.process._

class SparkSQLCallExternal extends SemanticRule("SparkSQLCallExternal") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val sparkSQLFunMatch = SymbolMatcher.normalized("org.apache.spark.sql.SparkSession.sql")
    val utils = new Utils()

    // sqlfluff is an optional external formatter -- not every environment
    // this jar runs in has it installed. Before this, a missing/failing
    // `sqlfluff` threw an IOException straight out of `fix`, which scalafix
    // reports as `ScalafixFailed: UnexpectedError` and fails the WHOLE
    // scalafixAll run -- every rule, every file -- over one SQL literal this
    // rule couldn't reformat. Same class of bug as AccumulatorUpgrade's
    // non-exhaustive match: one rule's failure shouldn't take down the run.
    def reformatted(s: Lit.String): Option[String] = Try {
      val f = File.createTempFile("magic", ".sql")
      f.deleteOnExit()
      val bw = new BufferedWriter(new FileWriter(f))
      bw.write(s.value.toString)
      bw.close()
      val strToRun = s"sqlfluff  fix --dialect sparksql -f ${f.toPath}"
      println(s"Running ${strToRun}")
      val ret = strToRun.!
      println(ret)
      scala.io.Source.fromFile(f).mkString
    }.toOption

    def matchOnTree(e: Tree): Patch = {
      e match {
        // non-named accumulator
        case ns @ Term.Apply(j @ sparkSQLFunMatch(f), params) =>
          // Find the spark context for rewriting
          params match {
            case List(param) =>
              param match {
                case s @ Lit.String(_) =>
                  reformatted(s) match {
                    // We don't care about whitespace only changes.
                    case Some(newSQL) if newSQL.filterNot(_.isWhitespace) != s =>
                      // Anchored at the END of the (possibly multi-line) string
                      // literal, not its start: a `// assert:` testkit comment
                      // can't be placed inside the literal without corrupting
                      // the SQL text, so it has to land on the line the
                      // literal closes on, not the line it opens on.
                      val endPos = Position.Range(param.pos.input, param.pos.end, param.pos.end)
                      RuleFinding.report(
                        RuleChange(
                          "SparkSQLCallExternal",
                          "sqlfluff found formatting/dialect issues in this literal SQL string.",
                          "Reformatted the SQL with sqlfluff's sparksql dialect.",
                          endPos
                        ),
                        Patch.replaceTree(param, "\"\"\"" + newSQL + "\"\"\"")
                      )
                    case _ =>
                      Patch.empty
                  }
                case _ =>
                  // TODO: Do we want to warn here about non migrated dynamically generated SQL
                  // or no?
                  Patch.empty
              }
            case _ =>
              Patch.empty
          }
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ => elem.children.map(matchOnTree).asPatch
          }
      }
    }
    matchOnTree(doc.tree)
  }
}
