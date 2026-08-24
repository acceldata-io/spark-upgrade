package fix

import org.apache.spark.executor.ShuffleWriteMetrics

object ShuffleWriteMetricsRename {
  def report(m: ShuffleWriteMetrics): String = {
    val b = m.bytesWritten
    val t = m.writeTime
    val r = m.recordsWritten
    // already-correct names must NOT be touched
    val ok = m.bytesWritten + m.writeTime + m.recordsWritten
    s"$b $t $r $ok"
  }
}
