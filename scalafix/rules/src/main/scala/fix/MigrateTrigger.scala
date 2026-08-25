package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._
import scala.meta._

/**
 * `org.apache.spark.sql.streaming.ProcessingTime` (the top-level object) was
 * removed in Spark 3.0 in favor of `Trigger.ProcessingTime(...)` -- a
 * same-named but distinct static forwarder owned by `Trigger`, not by
 * `ProcessingTime` itself, so the two are different symbols and a semantic
 * matcher targeting the removed object's own symbol does not collide with
 * the replacement API.
 *
 * The two gates below (`e.toString.contains("ProcessingTime")` and the
 * outer `doc.input.text.contains("org.apache.spark.sql.streaming")`) were
 * added to "deal with spurious matches", but neither is a semantic check:
 * the first tests the MATCHED node's own rendered text, which is always
 * `"ProcessingTime"` regardless of which real occurrence matched, so it can
 * never exclude anything; the second requires that literal package string
 * to appear verbatim in the file, so an aliased import
 * (`import org.apache.spark.sql.streaming.{ProcessingTime => PT}`, still
 * resolving to the exact same removed symbol) is silently missed. Both
 * removed -- the underlying `SymbolMatcher` was already precise.
 *
 * A second, more serious bug was caught during a later tier-promotion
 * review: adding `import org.apache.spark.sql.streaming.Trigger._` while
 * leaving the bare `ProcessingTime(...)` call site untouched does NOT work
 * when the file also has (as is typical) `import
 * org.apache.spark.sql.streaming._` in scope -- verified with a real 2.11
 * compile against the 2.4.8 jars that this produces a hard "reference to
 * ProcessingTime is ambiguous" error, since two wildcard imports providing
 * the same simple name is an ambiguity in Scala, not resolved by import
 * order. That's a real regression against PROJECT-GUIDE's "a Tier 1 rewrite
 * must compile under 2.4.8 too" rule -- it would have broken Phase A's own
 * sequential compilation the moment this rule ran on such a file. Fixed to
 * replace the call site itself with the fully-qualified
 * `org.apache.spark.sql.streaming.Trigger.ProcessingTime`, which resolves
 * unambiguously regardless of what else is wildcard-imported (also verified
 * by a real 2.11/2.4.8 compile) -- no import needs adding at all.
 *
 * A third bug surfaced while fixing the second: the custom `matchOnTree`
 * walker's `case triggerMatcher(t) => ...` matched at whatever tree level it
 * was first encountered top-down -- since a `SymbolMatcher` also matches the
 * enclosing `Term.Apply` (the standard multi-match trap, PROJECT-GUIDE A.9
 * #2), it matched the WHOLE `ProcessingTime(1.second)` call, not just the
 * bare name, so `Patch.replaceTree` on it silently dropped the `(1.second)`
 * argument. Narrowed to `Term.Name` specifically (as `IsRunningLocallyWarn`/
 * `ShuffleWriteMetricsRenameDetect` already do) so only the identifier is
 * replaced and the call's own arguments survive untouched.
 */
class MigrateTrigger extends SemanticRule("MigrateTrigger") {
  override val description =
    """Migrate Trigger."""
  override val isRewrite = true

  private val triggerMatcher = SymbolMatcher.normalized("org.apache.spark.sql.streaming.ProcessingTime")
  private val qualifiedReplacement = "org.apache.spark.sql.streaming.Trigger.ProcessingTime"

  override def fix(implicit doc: SemanticDocument): Patch = {
    def matchOnTree(e: Tree): Patch = {
      e match {
        case t: Term.Name if triggerMatcher.matches(t) =>
          RuleFinding.report(
            RuleChange(
              "MigrateTrigger",
              "org.apache.spark.sql.streaming.ProcessingTime was removed in favor of Trigger.ProcessingTime.",
              s"Rewrote to $qualifiedReplacement",
              t
            ),
            Patch.replaceTree(t, qualifiedReplacement)
          )
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
