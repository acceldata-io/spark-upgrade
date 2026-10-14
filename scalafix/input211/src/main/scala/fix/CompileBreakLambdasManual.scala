/*
rule = CompileBreakDetect
 */
package fix

import org.apache.spark.sql.Dataset

// Input-only: shapes whose type the rule cannot write, left as written
// (CompileBreakDetect.<Sub>.Manual). None compiles on Scala 2.12.
object CompileBreakLambdasManual {
  object Inner { case class Hidden(x: Int) }
  def overloaded(it: Iterator[String]): Unit = ()
  def overloaded(it: Iterator[Int], x: Int): Unit = ()

  def a(ds: Dataset[String]): Unit = ds.foreachPartition(_.foreach(println))// assert: CompileBreakDetect
  def b(ds: Dataset[String]): Unit = ds.foreachPartition(overloaded(_))// assert: CompileBreakDetect
  def c(ds: Dataset[Inner.Hidden]): Unit = ds.foreachPartition(it => println(it.size))// assert: CompileBreakDetect
}
