package fix

import java.io._

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._
import scala.util.Try
import scala.sys.process._

object SparkSQLCallExternal {

  /** Probed once per JVM, not once per SQL literal.
   *
   * The whole Family K rule set (13 rules) lives in the external `sqlfluff`
   * plugin, so if the binary isn't on PATH this rule contributes ZERO findings
   * -- and, before this, said nothing about it: `Try(...).toOption` swallowed
   * the process-launch failure, `stillFlagged` came back empty, and
   * findings.json/report.html looked exactly like a repo whose SQL was clean.
   * Silently reporting "no SQL issues" for a repo nobody actually checked is
   * the worst failure mode an effort-estimation tool has, so the unavailable
   * case now produces its own finding.
   *
   * Probing once also avoids paying two failed process spawns per SQL literal
   * on every machine that doesn't have the tool. */
  lazy val available: Boolean =
    Try(Process(Seq("sqlfluff", "--version")).!(ProcessLogger(_ => (), _ => ())) == 0).getOrElse(false)
}

class SparkSQLCallExternal extends SemanticRule("SparkSQLCallExternal") {

  // The full Family K rule set this fork's sqlfluff plugin (../../../sql,
  // package sqlfluff-plugin-sparksql-upgrade) implements. Explicit allowlist
  // rather than plain `sqlfluff fix`/`sqlfluff lint` with no `--rules`:
  // unrestricted, sqlfluff also applies/reports its own generic SQL style
  // rules, which would get misattributed to a Spark migration fix
  // (gap-analysis 2026-08-21 SS3.4).
  private val ruleCodes = Seq(
    "SPARKSQLCAST_L001", "RESERVEDROPERTIES_L002", "NOCHARS_L003", "FORMATSTRONEINDEX_L004",
    "SPARKSQL_L004", "SPARKSQL_L005", "GLOBALTEMPVIEW_L006", "HISTOGRAMNUMERIC_L007",
    "BYTEPADDING_L008", "BINARYCONV_L009", "CTEPRECEDENCE_L010", "TYPEDPARTITION_L011",
    "TRANSFORMDELIM_L012"
  ).mkString(",")

  // e.g. "L:   1 | P:   8 | HISTOGRAMNUMERIC_L007 | histogram_numeric(...)..."
  // -- sqlfluff's human-readable lint format. Not `--format json`: the rules
  // module has no JSON library dependency (deliberately lightweight, like
  // DependencyAnalyzer's own regex-over-text approach on the Scala side),
  // and several of the plugin's rules print debug lines to stdout ahead of
  // any structured output anyway (confirmed empirically), so a line-anchored
  // regex over the human-readable format is no less robust here than parsing
  // JSON would be, without the added dependency.
  // Ends in `.*`, not just the trailing `\|`: matching via `case lintLine(code) =>`
  // uses `Regex.unapplySeq`, which requires a FULL match of the whole line
  // (`Matcher.matches`, not `.find`) -- without consuming the rest of the
  // line (the description text after the last `|`), the match fails
  // entirely and every lint line is silently dropped (confirmed the hard
  // way: `stillFlagged` came back empty against real sqlfluff output that
  // plainly contained a matching line).
  private val lintLine = """^L:\s*\d+\s*\|\s*P:\s*\d+\s*\|\s*(\S+)\s*\|.*""".r

  override def fix(implicit doc: SemanticDocument): Patch = {
    // Both entry points, not just SparkSession: 2.4-era code routinely calls
    // `sqlContext.sql(...)`, and this jar's own MigrateHiveContext/
    // MigrateToSparkSessionBuilder rules deliberately REWRITE code into that
    // shape. Matching only SparkSession.sql meant the Family K pass skipped
    // exactly the call sites the rest of the jar produces -- the sibling SQL
    // detectors (SqlInStringDetect, MixedIntervalLiteralDetect, ...) already
    // name both.
    val sparkSQLFunMatch = SymbolMatcher.normalized(
      "org.apache.spark.sql.SparkSession.sql",
      "org.apache.spark.sql.SQLContext.sql"
    )
    // One finding per document is enough to say "Family K didn't run here";
    // one per SQL literal would bury the real findings.
    var reportedUnavailable = false
    val utils = new Utils()

    // sqlfluff is an optional external tool -- not every environment this
    // jar runs in has it installed. A missing/failing `sqlfluff` must not
    // throw out of `fix`: that would fail the WHOLE scalafixAll run over
    // one SQL literal this rule couldn't process. Same class of bug as
    // AccumulatorUpgrade's non-exhaustive match. `lineStream_!` (not `!!`)
    // deliberately: sqlfluff exits non-zero whenever it finds/reports
    // anything at all, which is the NORMAL case this rule exists to
    // capture, not a failure -- `!!` throws on any non-zero exit and would
    // discard exactly the output this rule needs. `lineStream_!`, not
    // `lazyLines_!`, since this module cross-builds for 2.11/2.12/2.13 and
    // `lazyLines_!` doesn't exist before 2.13.
    def sqlfluffOutput(args: Seq[String], sqlFile: File): Option[String] =
      Try(Process(Seq("sqlfluff") ++ args ++ Seq("--dialect", "sparksql", "--rules", ruleCodes, sqlFile.toPath.toString)).lineStream_!.mkString("\n")).toOption

    // Runs `fix` (applies whatever the plugin's fix-compatible rules can
    // rewrite, in place) then `lint` on the result (to see what's still
    // flagged -- the detect-only rules, which `fix` never touches). Returns
    // (rewritten SQL text, iff it actually changed; rule codes still
    // flagged after fixing).
    def analyze(original: String, sqlFile: File): (Option[String], Set[String]) = {
      sqlfluffOutput(Seq("fix", "-f"), sqlFile)
      val fixed = Try(scala.io.Source.fromFile(sqlFile).mkString).toOption
      val stillFlagged = sqlfluffOutput(Seq("lint"), sqlFile) match {
        case Some(output) => output.linesIterator.collect { case lintLine(code) => code }.toSet
        case None => Set.empty[String]
      }
      // We don't care about whitespace-only changes.
      val rewritten = fixed.filter(newSQL => newSQL.filterNot(_.isWhitespace) != original.filterNot(_.isWhitespace))
      (rewritten, stillFlagged)
    }

    def matchOnTree(e: Tree): Patch = {
      e match {
        case Term.Apply(sparkSQLFunMatch(_), params) =>
          params match {
            case List(param) =>
              param match {
                case Lit.String(sql) if !SparkSQLCallExternal.available =>
                  if (reportedUnavailable) Patch.empty
                  else {
                    reportedUnavailable = true
                    RuleFinding.report(
                      RuleChange(
                        "SparkSQLCallExternal",
                        "sqlfluff is not available on PATH, so the sparksql-upgrade plugin's Family K SQL rules did NOT run against " +
                          "the literal SQL in this file. Absence of SQL findings here means 'not checked', not 'clean'.",
                        "Install sqlfluff + the sqlfluff-plugin-sparksql-upgrade package (see spark-upgrade/sql) and re-run analysis.",
                        Position.Range(param.pos.input, param.pos.end, param.pos.end)
                      )
                    )
                  }
                case Lit.String(sql) =>
                  val f = File.createTempFile("magic", ".sql")
                  f.deleteOnExit()
                  val bw = new BufferedWriter(new FileWriter(f))
                  bw.write(sql)
                  bw.close()
                  val (rewritten, stillFlagged) = analyze(sql, f)

                  // Anchored at the END of the (possibly multi-line) string
                  // literal, not its start: a `// assert:` testkit comment
                  // can't be placed inside the literal without corrupting
                  // the SQL text, so it has to land on the line the literal
                  // closes on, not the line it opens on.
                  val endPos = Position.Range(param.pos.input, param.pos.end, param.pos.end)

                  // The rewrite is emitted as a triple-quoted literal, which
                  // silently produces unparseable Scala if the SQL itself ends
                  // in a double quote (`""""` closes early) or contains a
                  // `"""` sequence. Report those as review-only rather than
                  // writing source that no longer compiles.
                  val safeToInline = (s: String) => !s.contains("\"\"\"") && !s.endsWith("\"")
                  val rewritePatch = rewritten match {
                    case Some(newSQL) if safeToInline(newSQL) =>
                      RuleFinding.report(
                        RuleChange(
                          "SparkSQLCallExternal",
                          "sqlfluff found formatting/dialect issues in this literal SQL string.",
                          "Reformatted the SQL with sqlfluff's sparksql dialect.",
                          endPos
                        ),
                        Patch.replaceTree(param, "\"\"\"" + newSQL + "\"\"\"")
                      )
                    case Some(_) =>
                      RuleFinding.report(
                        RuleChange(
                          "SparkSQLCallExternal",
                          "sqlfluff rewrote this SQL, but the result can't be embedded back as a triple-quoted Scala literal " +
                            "(it ends in a double quote or contains a \"\"\" sequence).",
                          "No auto-rewrite; apply sqlfluff's suggested SQL by hand.",
                          endPos
                        )
                      )
                    case None => Patch.empty
                  }
                  val detectPatch =
                    if (stillFlagged.isEmpty) Patch.empty
                    else
                      RuleFinding.report(
                        RuleChange(
                          "SparkSQLCallExternal",
                          s"sqlfluff flagged this SQL for: ${stillFlagged.toSeq.sorted.mkString(", ")}. " +
                            "See spark-upgrade/sql's rule docs (sparksql_upgrade/rules.py) for what changed and why.",
                          "No auto-rewrite; review manually.",
                          endPos
                        )
                      )
                  List(rewritePatch, detectPatch).asPatch
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
