package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.meta._

/**
 * Tier 1 (core migration guide, 2.4 -> 3.0): the three deprecated
 * `ShuffleWriteMetrics` accessors were REMOVED in Spark 3.0, each in favour of
 * an exactly-equivalent shorter name. Verified against the real
 * `spark-core_2.11-2.4.8.jar` before writing this: all three exist there, and
 * their replacements exist there too -- so the rewrite is valid under 2.4.8 as
 * well and cannot break the pre-migration build.
 *
 * A pure rename with no semantic difference at all, which is the same bar
 * `UnionRewrite` meets, so this is auto-applied.
 */
class ShuffleWriteMetricsRenameDetect extends SemanticRule("ShuffleWriteMetricsRenameDetect") {
  override val description =
    "ShuffleWriteMetrics.shuffleBytesWritten/shuffleWriteTime/shuffleRecordsWritten were removed in Spark 3.0; renamed to bytesWritten/writeTime/recordsWritten."

  override val isRewrite = true

  private val renames = Map(
    "shuffleBytesWritten" -> "bytesWritten",
    "shuffleWriteTime" -> "writeTime",
    "shuffleRecordsWritten" -> "recordsWritten"
  )

  private val matcher = SymbolMatcher.normalized(
    renames.keys.toSeq.map(n => s"org.apache.spark.executor.ShuffleWriteMetrics.$n"): _*
  )

  // Matched on the `Term.Name` specifically, not on any enclosing Term.Select /
  // Term.Apply: a SymbolMatcher happily matches all three layers of the same
  // call, which is exactly why GroupByKeyWarn reports one position two or three
  // times (2026-08-17 review SS4.2). Narrowing to the name both fixes the
  // duplicate reporting and gives Patch.replaceTree the smallest correct span.
  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t: Term.Name if renames.contains(t.value) && matcher.matches(t) =>
        val replacement = renames(t.value)
        RuleFinding.report(
          RuleChange(
            "ShuffleWriteMetricsRenameDetect",
            s"ShuffleWriteMetrics.${t.value} was removed in Spark 3.0; $replacement is the exact equivalent.",
            s"Renamed ${t.value} to $replacement",
            t
          ),
          Patch.replaceTree(t, replacement)
        )
    }.asPatch
  }
}
