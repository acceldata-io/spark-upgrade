import org.apache.spark._
import org.apache.spark.sql._ 

class Wrapper(val ctx: SQLContext)

object BadHiveContextMagic {
  def hiveContextFunc(sc: SparkContext): SQLContext = { 
    val hiveContext1 = SparkSession.builder.enableHiveSupport().getOrCreate().sqlContext 
    import hiveContext1.implicits._
    hiveContext1
  }

  // A `new HiveContext(...)` nested inside another constructor's arguments --
  // must still be found even though the outer `new Wrapper(...)` itself
  // doesn't match HiveContext.
  def wrapped(sc: SparkContext): Wrapper = new Wrapper(SparkSession.builder.enableHiveSupport().getOrCreate().sqlContext) 

  def makeSparkConf() = {
    val sparkConf = new SparkConf(true)
    sparkConf
  }

  def throwSomeCrap() = {
    throw new RuntimeException("mr farts!")
  }
}
