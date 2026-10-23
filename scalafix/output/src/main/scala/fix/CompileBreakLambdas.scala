package fix

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.Row

case class Reading(id: String, value: Double)

// Scala 2.11: an untyped lambda passed to foreachPartition/foreachBatch, which
// Scala 2.12 cannot resolve. The rewrite writes the types 2.11 inferred.
object CompileBreakLambdas {
  def count(it: Iterator[Reading]): Unit = println(it.size)
  def process(batch: DataFrame, id: Long): Unit = println(id)

  def a(ds: Dataset[Reading]): Unit = ds.foreachPartition((it: Iterator[Reading]) => println(it.size))
  def b(ds: Dataset[Reading]): Unit = ds.foreachPartition { (it: Iterator[Reading]) =>
    it.foreach(r => println(r.id))
  }
  def c(df: DataFrame): Unit = df.foreachPartition((rows: Iterator[Row]) => println(rows.size))
  def d(ds: Dataset[(String, Int)]): Unit = ds.foreachPartition((it: Iterator[(String, Int)]) => println(it.size))
  def e(ds: Dataset[Reading]): Unit = ds.foreachPartition(count _)
  def f(df: DataFrame): Unit = df.writeStream.foreachBatch((b: DataFrame, id: Long) => println(id)).start()
  def g(df: DataFrame): Unit = df.writeStream.foreachBatch(process _).start()
  // Typed, but the body ends in a value (batch.unpersist()): still ambiguous on 2.12.
  def h(df: DataFrame): Unit = df.writeStream.foreachBatch { (batch: DataFrame, id: Long) =>
    batch.persist()
    batch.unpersist()
    ()
  }.start()
  def i(df: DataFrame): Unit = df.writeStream.foreachBatch((batch: DataFrame, id: Long) => { batch.unpersist(); () }).start()
  def j(df: DataFrame): Unit = df.writeStream.foreachBatch { (batch: DataFrame, id: Long) =>
    println(id)
  }.start()
}
