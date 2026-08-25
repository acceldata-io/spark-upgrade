/*
 rule=MigrateHiveContext
 */
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext // assert: MigrateHiveContext

class Wrapper(val ctx: SQLContext)

object BadHiveContextMagic {
  def hiveContextFunc(sc: SparkContext): HiveContext = { // assert: MigrateHiveContext
    val hiveContext1 = new HiveContext(sc) // assert: MigrateHiveContext
    import hiveContext1.implicits._
    hiveContext1
  }

  // A `new HiveContext(...)` nested inside another constructor's arguments --
  // must still be found even though the outer `new Wrapper(...)` itself
  // doesn't match HiveContext.
  def wrapped(sc: SparkContext): Wrapper = new Wrapper(new HiveContext(sc)) // assert: MigrateHiveContext

  def makeSparkConf() = {
    val sparkConf = new SparkConf(true)
    sparkConf
  }

  def throwSomeCrap() = {
    throw new RuntimeException("mr farts!")
  }
}
