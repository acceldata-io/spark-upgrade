/*
 rule=MigrateHiveContext
 */
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext // assert: MigrateHiveContext

object BadHiveContextMagic {
  def hiveContextFunc(sc: SparkContext): HiveContext = { // assert: MigrateHiveContext
    val hiveContext1 = new HiveContext(sc) // assert: MigrateHiveContext
    import hiveContext1.implicits._
    hiveContext1
  }

  def makeSparkConf() = {
    val sparkConf = new SparkConf(true)
    sparkConf
  }

  def throwSomeCrap() = {
    throw new RuntimeException("mr farts!")
  }
}
