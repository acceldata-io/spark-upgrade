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
  /** The full Family K rule set this fork's sqlfluff plugin (../../../sql,
   * package sqlfluff-plugin-sparksql-upgrade) implements. Lives on the
   * companion so `available` can probe with the SAME allowlist the rule
   * actually uses. */
  val pluginRuleCodes: Set[String] = Set(
    "SPARKSQLCAST_L001", "RESERVEDROPERTIES_L002", "NOCHARS_L003", "FORMATSTRONEINDEX_L004",
    "SPARKSQL_L004", "SPARKSQL_L005", "GLOBALTEMPVIEW_L006", "HISTOGRAMNUMERIC_L007",
    "BYTEPADDING_L008", "BINARYCONV_L009", "CTEPRECEDENCE_L010", "TYPEDPARTITION_L011",
    "TRANSFORMDELIM_L012"
  )

  val ruleCodes: String = pluginRuleCodes.toSeq.sorted.mkString(",")

  lazy val available: Boolean = probe().isEmpty

  /** `None` when Family K can really run; `Some(reason)` otherwise.
   *
   * This used to probe `sqlfluff --version`, which was not the same question.
   * Two real environments pass `--version` and still produce zero Family K
   * findings:
   *
   *   1. **`sqlfluff` runs but `lint` crashes.** sqlfluff 2.3.2's
   *      `click_deprecated_option` raises `ValueError: Expected `deprecated`
   *      value for 'disable_progress_bar'` under click >= 8.2, during
   *      argument parsing -- so `--version` is fine and every `lint`/`fix`
   *      dies with a traceback and exit 1, which is indistinguishable by exit
   *      code from the normal "found violations" case. Reproduced 2026-09-24
   *      in the venv PROJECT-GUIDE.md SS0.4 step 3b tells you to build:
   *      installing `pysparkler` alongside the plugin resolves click 8.5.0,
   *      while a plugin-only venv resolves 8.1.8 and works. The tool then
   *      reported a Family K-heavy fixture as having zero SQL findings, and
   *      `analyze` said SUCCESS.
   *   2. **`sqlfluff` runs but the plugin is missing or stale.** PyPI's
   *      published plugin ships only 6 of these 13 codes (same SS0.4 note);
   *      an unregistered code is a `WARNING Tried to allowlist unknown rule
   *      references: [...]` line, not an error, so the run finishes clean.
   *
   * Both are exactly the failure this rule's own doc comment above calls "the
   * worst failure mode an effort-estimation tool has". Probing with a real
   * one-line `lint` against the real allowlist answers the real question:
   * sqlfluff's human format always terminates with `All Finished!`, and names
   * any code it could not resolve. */
  private def probe(): Option[String] = {
    val probeFile = File.createTempFile("sqlfluff-probe", ".sql")
    probeFile.deleteOnExit()
    val bw = new BufferedWriter(new FileWriter(probeFile))
    bw.write("SELECT 1\n")
    bw.close()

    val out = new StringBuilder
    val ran = Try {
      Process(Seq("sqlfluff", "lint", "--dialect", "sparksql", "--rules", ruleCodes, probeFile.toPath.toString))
        .!(ProcessLogger(l => { out.append(l).append('\n'); () }, _ => ()))
      out.toString
    }.toOption

    ran match {
      case None =>
        Some("sqlfluff is not on PATH")
      case Some(text) if !text.contains("All Finished!") =>
        Some("sqlfluff is on PATH but could not complete a `lint` run (it exited without finishing -- " +
          "commonly a click/sqlfluff version incompatibility; run the command by hand to see the traceback)")
      case Some(text) if text.contains("Tried to allowlist unknown rule references") =>
        Some("sqlfluff is on PATH but the sparksql-upgrade plugin is missing or stale -- it does not " +
          "register all 13 Family K rule codes")
      case Some(_) => None
    }
  }

  /** The reason Family K could not run, for the finding's own text. */
  lazy val unavailableReason: String = probe().getOrElse("")
}

class SparkSQLCallExternal extends SemanticRule("SparkSQLCallExternal") {

  // Explicit allowlist rather than plain `sqlfluff fix`/`sqlfluff lint` with
  // no `--rules`: unrestricted, sqlfluff also applies/reports its own generic
  // SQL style rules, which would get misattributed to a Spark migration fix
  // (gap-analysis 2026-08-21 SS3.4). The list itself lives on the companion
  // so the availability probe can use the same one.
  private val pluginRuleCodes: Set[String] = SparkSQLCallExternal.pluginRuleCodes

  private val ruleCodes = SparkSQLCallExternal.ruleCodes

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

    // `lint` runs FIRST, against the ORIGINAL file, so the codes it collects
    // are the actual violations in the code as written -- then `fix` runs
    // and may rewrite the file in place. Doing it in the other order (fix
    // then lint) meant every auto-fixable code's violation was already gone
    // by the time lint ran, so it never appeared in the flagged set -- only
    // the 7 detect-only codes (which `fix` never touches) survived. Worse,
    // a fix that introduces a new shape (e.g. SPARKSQL_L005 wrapping the
    // accuracy arg in `cast(...)`) got that new shape flagged by the
    // post-fix lint as a DIFFERENT code (SPARKSQLCAST_L001) -- a
    // self-referential false positive on code that was just correctly
    // fixed. Linting the original avoids both.
    def analyze(original: String, sqlFile: File): (Option[String], Set[String]) = {
      val flagged = sqlfluffOutput(Seq("lint"), sqlFile) match {
        case Some(output) => output.linesIterator.collect { case lintLine(code) => code }.toSet
        case None => Set.empty[String]
      }
      sqlfluffOutput(Seq("fix", "-f"), sqlFile)
      val fixed = Try(scala.io.Source.fromFile(sqlFile).mkString).toOption
      // We don't care about whitespace-only changes.
      val rewritten = fixed.filter(newSQL => newSQL.filterNot(_.isWhitespace) != original.filterNot(_.isWhitespace))
      (rewritten, flagged)
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
                        s"Family K SQL rules did NOT run against the literal SQL in this file: " +
                          s"${SparkSQLCallExternal.unavailableReason}. Absence of SQL findings here means " +
                          "'not checked', not 'clean'.",
                        "Install/repair sqlfluff + the sqlfluff-plugin-sparksql-upgrade package (see spark-upgrade/sql) and re-run analysis.",
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
                  // sqlfluff reports its own PRS (parse) violations regardless
                  // of `--rules`, so they arrive mixed in with the plugin's
                  // codes. They mean something completely different -- "the
                  // sparksql dialect could not parse this literal", i.e. the
                  // Family K rules never got to evaluate it -- and reporting
                  // that as "flagged for PRS" is worse than saying nothing.
                  // Split the two and word each honestly. (Confirmed against a
                  // real run: the `FROM t` form, itself a 3.0 break, is
                  // unparsable to sqlfluff and surfaced only as PRS.)
                  val (pluginCodes, parserCodes) = stillFlagged.partition(pluginRuleCodes.contains)

                  val unparseablePatch =
                    if (parserCodes.isEmpty) Patch.empty
                    else
                      RuleFinding.report(
                        RuleChange(
                          "SparkSQLCallExternal.UNPARSEABLE",
                          "sqlfluff's sparksql dialect could not parse this SQL literal, so NONE of the Family K " +
                            "SQL rules were evaluated against it -- treat this as 'not checked', not 'clean'. " +
                            "Common causes: syntax Spark accepts but the dialect doesn't model yet, and syntax " +
                            "Spark 3.x itself rejects (a leading `FROM`, for instance, which FromWithoutSelectDetect " +
                            "reports separately).",
                          "No auto-rewrite; review this SQL by hand.",
                          endPos
                        )
                      )

                  // ONE finding per flagged plugin rule code, and the code is
                  // carried in the finding id as `SparkSQLCallExternal.<CODE>`.
                  //
                  // Previously all 13 of the plugin's rules collapsed into a
                  // single `SparkSQLCallExternal` finding with the codes listed
                  // in prose. That made them individually unaddressable
                  // downstream: several of these SQL behaviour changes have
                  // their own legacy config (CTEPRECEDENCE_L010 ->
                  // spark.sql.legacy.ctePrecedencePolicy,
                  // RESERVEDROPERTIES_L002 -> ...notReserveProperties), and the
                  // orchestrator maps a config to a RULE ID -- so with one
                  // shared id it could neither tell which change was found nor
                  // key a config off it. See the 2026-08-22 review SS6.3.2.
                  val detectPatch =
                    pluginCodes.toSeq.sorted.map { code =>
                      RuleFinding.report(
                        RuleChange(
                          s"SparkSQLCallExternal.$code",
                          s"sqlfluff's sparksql-upgrade plugin flagged this SQL for $code. " +
                            "See spark-upgrade/sql's rule docs (sparksql_upgrade/rules.py) for what changed and why.",
                          "No auto-rewrite; review manually.",
                          endPos
                        )
                      )
                    }.asPatch
                  List(rewritePatch, detectPatch, unparseablePatch).asPatch
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
