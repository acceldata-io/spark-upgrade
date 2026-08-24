/*
rule = ShuffleWriteMetricsRenameDetect
 */
package fix

import org.apache.spark.executor.ShuffleWriteMetrics

object ShuffleWriteMetricsRename {
  def report(m: ShuffleWriteMetrics): String = {
    val b = m.shuffleBytesWritten// assert: ShuffleWriteMetricsRenameDetect
    val t = m.shuffleWriteTime// assert: ShuffleWriteMetricsRenameDetect
    val r = m.shuffleRecordsWritten// assert: ShuffleWriteMetricsRenameDetect
    // already-correct names must NOT be touched
    val ok = m.bytesWritten + m.writeTime + m.recordsWritten
    s"$b $t $r $ok"
  }
}
