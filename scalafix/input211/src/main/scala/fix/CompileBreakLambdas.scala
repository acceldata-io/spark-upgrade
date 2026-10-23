/*
rule = CompileBreakDetect
 */
package fix

import org.apache.spark.sql.{DataFrame, Dataset}

case class Reading(id: String, value: Double)

// Scala 2.11: an untyped lambda passed to foreachPartition/foreachBatch, which
// Scala 2.12 cannot resolve. The rewrite writes the types 2.11 inferred.
object CompileBreakLambdas {
  def count(it: Iterator[Reading]): Unit = println(it.size)
  def process(batch: DataFrame, id: Long): Unit = println(id)

  def a(ds: Dataset[Reading]): Unit = ds.foreachPartition(it => println(it.size))// assert: CompileBreakDetect
  def b(ds: Dataset[Reading]): Unit = ds.foreachPartition { it =>// assert: CompileBreakDetect
    it.foreach(r => println(r.id))
  }
  def c(df: DataFrame): Unit = df.foreachPartition((rows) => println(rows.size))// assert: CompileBreakDetect
  def d(ds: Dataset[(String, Int)]): Unit = ds.foreachPartition(it => println(it.size))// assert: CompileBreakDetect
  def e(ds: Dataset[Reading]): Unit = ds.foreachPartition(count(_))// assert: CompileBreakDetect
  def f(df: DataFrame): Unit = df.writeStream.foreachBatch((b, id) => println(id)).start()// assert: CompileBreakDetect
  def g(df: DataFrame): Unit = df.writeStream.foreachBatch(process(_, _)).start()// assert: CompileBreakDetect
  // Typed, but the body ends in a value (batch.unpersist()): still ambiguous on 2.12.
  def h(df: DataFrame): Unit = df.writeStream.foreachBatch { (batch: DataFrame, id: Long) =>// assert: CompileBreakDetect
    batch.persist()
    batch.unpersist()
  }.start()
  def i(df: DataFrame): Unit = df.writeStream.foreachBatch((batch: DataFrame, id: Long) => batch.unpersist()).start()// assert: CompileBreakDetect
  def j(df: DataFrame): Unit = df.writeStream.foreachBatch { (batch: DataFrame, id: Long) =>
    println(id)
  }.start()
}
